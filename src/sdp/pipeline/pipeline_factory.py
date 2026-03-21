import logging
from typing import Any, List, Dict
from queue import Queue

from models.pipeline import PipelineConfig, StepConfig, StepType

logger = logging.getLogger(__name__)

class ProcessingStep:
    """
    Base class for all processing steps in a processing pipeline.
    Each step should implement the process method.
    """
    def __init__(self, config: StepConfig = None):

        if config is None or not isinstance(config, StepConfig):
            raise ValueError(f"ProcessingStep: step config {config} is None or not a StepConfig instance. Cannot initialise the step.")

        self.config = config

    def process(self, context: Any, signal: Any) -> Any:
        """
        Process the signal and return the processed signal.
        Override this method in subclasses.
        """
        raise NotImplementedError("process() must be implemented by subclasses.")

class ProcessingPipeline:
    """
    Manages a sequence of ProcessingStep objects and applies them to a signal.
    """
    
    def __init__(self, steps: List[ProcessingStep] = None):
        self.steps = steps or []

    def add_step(self, step: ProcessingStep):
        self.steps.append(step)

    def process(self, context: Any, signal: Any) -> Any:
        for step in self.steps:
            signal = step.process(context=context, signal=signal)
        return signal

class ProcessingPipelineFactory:
    """
    Factory to create ProcessingPipeline instances for a given digitiser, using PipelineConfig.
    """
    def __init__(self, pipeline_config: PipelineConfig):
        self.pipeline_config = pipeline_config

    def get_steps_for_dig(self, scan: "Scan" = None, scan_q: Queue = None, cal_q: Queue = None) -> List[ProcessingStep]:
        """ 
        Get the list of ProcessingStep instances for the given digitiser ID based on the pipeline configuration.
        If there are no specific steps for the dig_id, it will fall back to the 'default' steps.
        """
        step_configs = self.pipeline_config.get_steps(scan.scan_model.dig_id)

        for config in step_configs:
            config.params['scan'] = scan        # The scan that the pipeline will process
            config.params['scan_q'] = scan_q    # Pipeline steps are provided access to the scan queue if needed
            config.params['cal_q'] = cal_q      # Pipeline steps are provided access to the calibration queue if needed

        return [self.instantiate_step(config) for config in step_configs]

    def instantiate_step(self, config: StepConfig) -> ProcessingStep:
        """
        Instantiate a processing step based on its StepConfig.
        cfg is a StepConfig instance.
        """
        # Import step classes here to avoid circular imports
        from sdp.pipeline.steps.nop      import Nop
        from sdp.pipeline.steps.dc_spike import DCSpike
        from sdp.pipeline.steps.load     import LoadCal
        from sdp.pipeline.steps.gain     import GainCal
        from sdp.pipeline.steps.tsys     import TsysCal
        from sdp.pipeline.steps.rfi      import RFIFlag

        step_map = {
            StepType.NOP.value:      Nop,
            StepType.DC_SPIKE.value: DCSpike,
            StepType.LOAD.value:     LoadCal,
            StepType.GAIN_CAL.value: GainCal,
            StepType.TSYS_CAL.value: TsysCal,
            StepType.RFI_FLAG.value: RFIFlag,
            # Add more step types as needed
        }

        # Return an instance of the step class (passing config to the constructor)
        return step_map[config.step.value](config)

    def create_pipeline(self, scan: "Scan", scan_q: Queue, cal_q: Queue):
        steps = self.get_steps_for_dig(scan, scan_q, cal_q)
        return ProcessingPipeline(steps)
