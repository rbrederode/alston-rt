from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sdp.scan import Scan
    
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

from matplotlib.gridspec import GridSpec
from queue import Queue

import logging
logger = logging.getLogger(__name__)

FIG_SIZE = (14, 7)  # Default figure size for plots

class QA:

    def __init__(self):

        self.scan = None        # Current scan being displayed

        self.fig = None         # Figure for the signal displays    
        self.sig = [None] * 5   # Axes for the signal subplots

        
    def set_scan(self, scan : Scan):
        """ # Initialize the figure and axes of the signal displays for a given scan
            :param scan: The Scan object containing the data to display
        """
        self.scan = scan

        self.fig = plt.figure(num=scan.id, figsize=FIG_SIZE)
        self.sig = [None] * 5  # Initialize axes for the subplots
