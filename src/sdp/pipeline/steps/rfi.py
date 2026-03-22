import numpy as np
import logging
from typing import Any, List, Dict

from models.pipeline import StepConfig, StepType
from sdp.pipeline.pipeline_factory import ProcessingStep

logger = logging.getLogger(__name__)

class RFIFlag(ProcessingStep):

    def __init__(self, config: StepConfig = None):
        super().__init__(config)

        logger.info("RFIFlag pipeline step initialisation with config:\n%s", str(self.config))
    
    def process(self, context: Any, signal: Any) -> Any:
        """
        Apply sliding-window MAD RFI flagging to the signal array, modifying it in-place.
        """
        if not isinstance(signal, np.ndarray):
            raise ValueError("RFIFlag: signal must be a numpy array.")
        if not isinstance(context, dict):
            raise ValueError("RFIFlag: context must be a dictionary.")

        n = context.get("threshold", 5)
        window_size = context.get("window_size", 21)  # Must be odd
        if window_size % 2 == 0:
            raise ValueError("window_size must be odd.")

        half_window = window_size // 2
        num_flagged = 0

        for i in range(len(signal)):
            start = max(0, i - half_window)
            end = min(len(signal), i + half_window + 1)
            window = signal[start:end]
            median = np.median(window)
            mad = np.median(np.abs(window - median))
            threshold = n * mad
            if np.abs(signal[i] - median) > threshold:
                signal[i] = median
                num_flagged += 1

        logger.info(f"RFIFlag (sliding window): Flagged {num_flagged} channels as RFI outliers using window_size={window_size}, threshold={n}*MAD")
        return signal

def main():
 
    # Set log level to info for demonstration
    logging.basicConfig(level=logging.INFO)

    # Example StepConfig for gain calibration
    step_config = StepConfig(step=StepType.RFI_FLAG, params={"threshold": 5, "window_size": 21})
    rfi_step = RFIFlag(step_config)

    # Example signal and context
    import numpy as np
    signal = np.random.rand(1024)
    print("Original signal: ", signal)
    context = {"channels": 1024, "rfi": 10}

    processed_signal = rfi_step.process(context, signal)
    print("Processed signal:", processed_signal)

if __name__ == "__main__":
    main()

