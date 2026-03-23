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

        self.scan = config.params["scan"] if "scan" in config.params else None
        self.scan_qa = self.scan.get_qa() if self.scan else None

        logger.debug("RFIFlag pipeline step initialisation with scan qa:\n%s", str(self.scan_qa))

        if self.scan_qa is None:
            raise ValueError(f"RFIFlag: scan_qa {self.scan_qa} must be set before initialising RFIFlag step.")
    
    def process(self, context: Any, signal: Any) -> Any:
        """
        Apply sliding-window MAD RFI flagging to the signal array, modifying it in-place.
        """
        if not isinstance(signal, np.ndarray):
            raise ValueError("RFIFlag: signal must be a numpy array.")

        if not isinstance(context, dict):
            raise ValueError("RFIFlag: context must be a dictionary.")

        pipeline = context.get("pipeline", "unknown")  # Get the pipeline name from the context 

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

        # --- Update scan QA attributes
        sec = self.scan.get_loaded_seconds()
        qa = self.scan_qa.getQA(pipeline, sec)

        qa.rfi_fraction = num_flagged / len(signal) if len(signal) > 0 else 0.0

        logger.info(f"RFIFlag (sliding window): Flagged {num_flagged} channels as RFI outliers using window_size={window_size}, threshold={n}*MAD")
        return signal

def main():
 
    # Set log level to info for demonstration
    logging.basicConfig(level=logging.INFO)
    
    from queue import Queue
 
    scan_q = Queue()  # Set the calibration queue in the pipeline factory to None
    cal_q = Queue()   # Set the calibration queue in the pipeline factory to None

    from models.scan import ScanModel, ScanState
    from datetime import datetime, timezone

    scan001 = ScanModel(
        dig_id="dig001",
        obs_id="obs001",
        tgt_idx=0,
        freq_scan=1,
        scan_iter=5,
        created=datetime.now(timezone.utc),
        read_start=datetime.now(timezone.utc),
        read_end=datetime.now(timezone.utc),
        start_idx=100,
        duration=60,
        sample_rate=1024.0,
        channels=1024,
        center_freq=1420405752.0,
        gain=50.0,
        load=False,
        status=ScanState.WIP,
        load_failures=0,
        last_update=datetime.now(timezone.utc)
    )

    from obs.scan import Scan

    scan = Scan(scan_model=scan001)
    scan_q.put(scan)  # Put the scan in the scan queue for processing

    load001 = scan001.copy()
    load001.load = True
    load_scan = Scan(scan_model=load001)
    load_scan.mpr = load_scan.mpr / 0.5
    cal_q.put(load_scan)  # Put the load scan in the cal queue for processing

    params={}
    params['scan'] = scan     # The scan that the pipeline will process
    params['scan_q'] = scan_q    # Pipeline steps are provided access to the scan queue if needed
    params['cal_q'] = cal_q      # Pipeline steps are provided access to the calibration queue if needed
    params['threshold'] = 5      # Threshold for RFI flagging
    params['window_size'] = 21   # Window size for RFI flagging (must be odd)

    # Example StepConfig for RFI flagging
    step_config = StepConfig(step=StepType.RFI_FLAG, params=params)
    rfi_step = RFIFlag(step_config)

    # Example signal and context
    import numpy as np
    signal = np.random.rand(1024)
    print("Original signal: ", signal)
    context = {"pipeline": "cal", "channels": 1024, "rfi": 10}

    processed_signal = rfi_step.process(context, signal)
    print("Processed signal:", processed_signal)

    print("Scan QA after RFI flagging:")
    print(scan.scan_qa)

if __name__ == "__main__":
    main()

