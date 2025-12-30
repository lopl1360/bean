from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional

from core.models import BarEvent, Detection, MarketEvent
from detectors.base import Detector


@dataclass
class _State:
    bars: Deque[BarEvent]
    last_trigger_bar_count: int = 0


class BullFlagDetector(Detector):
    """
    Bull Flag detector (bars-based).

    Definition (practical):
      1) Flagpole: strong up move over a short window (impulse_len bars)
      2) Flag: tight consolidation, slight down/sideways drift (flag_len bars)
      3) Breakout: current close breaks above flag high by a small buffer
         and (if volume available) breakout volume > avg flag volume * multiplier

    Use 1m or 5m bars. Trades are too noisy.
    """

    name = "bull_flag"
    required_channels = {"bars"}

    def __init__(
        self,
        *,
        max_bars: int = 250,
        impulse_len: int = 12,
        flag_len: int = 10,
        min_impulse_pct: float = 2.0,
        max_flag_retrace_pct: float = 50.0,
        max_flag_range_pct: float = 1.2,
        max_flag_slope_pct_per_bar: float = 0.15,
        breakout_buffer_pct: float = 0.05,
        min_breakout_volume_mult: float = 1.5,
        cooldown_bars: int = 25,
        timeframe_allowlist: Optional[List[str]] = None,  # e.g. ["1m","5m"]
    ) -> None:
        self.max_bars = max_bars
        self.impulse_len = impulse_len
        self.flag_len = flag_len

        self.min_impulse_pct = min_impulse_pct
        self.max_flag_retrace_pct = max_flag_retrace_pct
        self.max_flag_range_pct = max_flag_range_pct
        self.max_flag_slope_pct_per_bar = max_flag_slope_pct_per_bar

        self.breakout_buffer_pct = breakout_buffer_pct
        self.min_breakout_volume_mult = min_breakout_volume_mult
        self.cooldown_bars = cooldown_bars

        self.timeframe_allowlist = timeframe_allowlist

        self._state: Dict[str, _State] = {}

    async def on_event(self, event: MarketEvent) -> List[Detection]:
        if not isinstance(event, BarEvent):
            return []

        if self.timeframe_allowlist is not None and event.timeframe not in self.timeframe_allowlist:
            return []

        st = self._state.get(event.symbol)
        if st is None:
            st = _State(bars=deque(maxlen=self.max_bars))
            self._state[event.symbol] = st

        st.bars.append(event)
        bars_list = list(st.bars)

        needed = self.impulse_len + self.flag_len + 1
        if len(bars_list) < needed:
            return []

        # Cooldown to avoid firing repeatedly on every bar above breakout
        if (len(bars_list) - st.last_trigger_bar_count) < self.cooldown_bars:
            return []

        # Slice windows: impulse -> flag -> breakout
        impulse = bars_list[-(self.flag_len + self.impulse_len + 1) : -(self.flag_len + 1)]
        flag = bars_list[-(self.flag_len + 1) : -1]
        breakout = bars_list[-1]

        # -------------------------
        # 1) Flagpole / impulse
        # -------------------------
        impulse_low = min(b.low for b in impulse)
        impulse_high = max(b.high for b in impulse)
        if impulse_low <= 0:
            return []

        impulse_pct = (impulse_high - impulse_low) / impulse_low * 100.0
        if impulse_pct < self.min_impulse_pct:
            return []

        # -------------------------
        # 2) Flag: tight consolidation
        # -------------------------
        flag_low = min(b.low for b in flag)
        flag_high = max(b.high for b in flag)
        if flag_low <= 0:
            return []

        flag_range_pct = (flag_high - flag_low) / flag_low * 100.0
        if flag_range_pct > self.max_flag_range_pct:
            return []

        # Retracement check (how deep flag pulls back relative to impulse size)
        impulse_size = impulse_high - impulse_low
        if impulse_size <= 0:
            return []

        retrace = impulse_high - flag_low
        retrace_pct = retrace / impulse_size * 100.0
        if retrace_pct > self.max_flag_retrace_pct:
            return []

        # Slope check: flag should not be strongly rising (that’s not a flag)
        first_close = flag[0].close
        last_close = flag[-1].close
        if first_close <= 0:
            return []

        slope_pct_total = (last_close - first_close) / first_close * 100.0
        slope_pct_per_bar = slope_pct_total / max(1, (len(flag) - 1))
        if slope_pct_per_bar > self.max_flag_slope_pct_per_bar:
            return []

        # -------------------------
        # 3) Breakout
        # -------------------------
        breakout_threshold = flag_high * (1.0 + self.breakout_buffer_pct / 100.0)
        if breakout.close <= breakout_threshold:
            return []

        # Breakout volume confirmation (only if volume looks usable)
        avg_flag_vol = sum(b.volume for b in flag) / len(flag)
        if avg_flag_vol > 0 and breakout.volume > 0:
            if breakout.volume < (avg_flag_vol * self.min_breakout_volume_mult):
                return []

        st.last_trigger_bar_count = len(bars_list)

        return [
            Detection(
                symbol=breakout.symbol,
                detector_name=self.name,
                severity="info",
                message=(
                    f"Bull flag breakout on {breakout.timeframe}: "
                    f"impulse={impulse_pct:.2f}% retrace={retrace_pct:.1f}% "
                    f"flag_range={flag_range_pct:.2f}% close={breakout.close:.4f} "
                    f"> {breakout_threshold:.4f}"
                ),
                timestamp=breakout.timestamp,
                data={
                    "timeframe": breakout.timeframe,
                    "impulse_pct": impulse_pct,
                    "impulse_low": impulse_low,
                    "impulse_high": impulse_high,
                    "flag_low": flag_low,
                    "flag_high": flag_high,
                    "flag_range_pct": flag_range_pct,
                    "retrace_pct": retrace_pct,
                    "slope_pct_per_bar": slope_pct_per_bar,
                    "breakout_close": breakout.close,
                    "breakout_threshold": breakout_threshold,
                    "avg_flag_vol": avg_flag_vol,
                    "breakout_vol": breakout.volume,
                },
            )
        ]
