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
        Apply RFI flagging to the signal array using parameters from context. 
        Uses a simple median absolute deviation (MAD) method to identify and flag outliers. 

       Args:
            context: dict containing static parameters for flagging RFI
            input_signal: 1D numpy array containing input spectrum (signal)
        Returns:
            1D numpy array containing processed output spectrum (signal)
        """

        if not isinstance(signal, np.ndarray):
            raise ValueError("RFIFlag: signal must be a numpy array.")

        if not isinstance(context, dict):
            raise ValueError("RFIFlag: context must be a dictionary.")

        n = context.get("threshold", 5)  # Default to 5 if not provided
        # Compute the median of the input signal.
        median = np.median(signal)
        # Calculate the MAD: median of the absolute deviations from the median.
        mad = np.median(np.abs(signal - median))
        # Define a threshold (e.g., n times MAD).
        threshold = n * mad
        # Flag or clip values that deviate from the median by more than the threshold.
        mask = np.abs(signal - median) > threshold

        # Log the number of flagged channels
        num_flagged = np.sum(mask)
        logger.info(f"RFIFlag: Flagged {num_flagged} channels as RFI outliers using threshold {threshold:.2f} (median={median:.2f}, MAD={mad:.2f})")

        signal[mask] = median  # Replace outliers with median
        return signal

def main():
 
    # Set log level to info for demonstration
    logging.basicConfig(level=logging.INFO)

    # Example StepConfig for gain calibration
    step_config = StepConfig(step=StepType.RFI_FLAG, params={"threshold": 5})
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

