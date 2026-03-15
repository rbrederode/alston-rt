import numpy as np
from sdp.pipeline_factory import PipelineFactory

class PipelineQA:
    """
    Signal QA class that uses a configurable processing pipeline.
    """
    def __init__(self, raw_spectrum: np.ndarray, load_spectrum: np.ndarray = None, pipeline_factory: PipelineFactory = None, dig_id: str = None):
        self.raw_spectrum = raw_spectrum.copy()
        self.load_spectrum = load_spectrum.copy() if load_spectrum is not None else None
        self.pipeline_factory = pipeline_factory
        self.dig_id = dig_id
        self.pipeline = None
        self.processed_spectrum = None
        self.snr = None
        self._init_pipeline()

    def _init_pipeline(self):
        if self.pipeline_factory and self.dig_id:
            self.pipeline = self.pipeline_factory.create_pipeline(self.dig_id)
        else:
            self.pipeline = None

    def process(self):
        signal = self.raw_spectrum.copy()
        if self.pipeline:
            signal = self.pipeline.process(signal)
        self.processed_spectrum = signal
        return signal

    def calc_snr(self, window_frac: float = 0.03):
        spectrum = self.processed_spectrum if self.processed_spectrum is not None else self.raw_spectrum
        channels = spectrum.shape[-1]
        peak_bin = np.argmax(spectrum)
        window_width = max(3, int(window_frac * channels))
        half_width = window_width // 2
        start = max(0, peak_bin - half_width)
        end = min(channels, peak_bin + half_width)
        signal_window = spectrum[start:end]
        noise_window = np.concatenate([spectrum[:start], spectrum[end:]])
        signal = np.mean(signal_window)
        noise = np.std(noise_window)
        self.snr = signal / noise if noise > 0 else np.inf
        return self.snr
