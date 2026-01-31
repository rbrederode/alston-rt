import matplotlib.pyplot as plt

try:
    from AppKit import NSApplication
    HAS_APPKIT = True
except ImportError:
    HAS_APPKIT = False
    print("AppKit not available. Install pyobjc: pip install pyobjc")

active_figure = None

def check_active_figure():
    """Poll to detect which figure window is currently active using AppKit."""
    global active_figure
    if not HAS_APPKIT:
        return
    
    key_window = NSApplication.sharedApplication().keyWindow()
    if key_window is None:
        return

    # Get the title of the key window
    key_window_title = key_window.title()

    for fig_num in plt.get_fignums():
        fig = plt.figure(fig_num)
        # Compare window titles (matplotlib uses the figure number or suptitle as window title)
        fig_title = fig.canvas.manager.get_window_title()
        if fig_title == key_window_title:
            if active_figure != fig_num:
                active_figure = fig_num
                print(f'Active figure changed to: {fig.get_suptitle()} (window: {fig_title})')
            return

def on_enter_axes(event):
    print('enter_axes', event.inaxes)
    event.inaxes.patch.set_facecolor('yellow')
    event.canvas.draw()

def on_leave_axes(event):
    print('leave_axes', event.inaxes)
    event.inaxes.patch.set_facecolor('white')
    event.canvas.draw()

def on_enter_figure(event):
    print('enter_figure', event.canvas.figure)
    event.canvas.figure.patch.set_facecolor('red')
    print(f'Active figure: {event.canvas.figure.get_suptitle()}')
    event.canvas.draw()

def on_leave_figure(event):
    print('leave_figure', event.canvas.figure)
    event.canvas.figure.patch.set_facecolor('grey')
    event.canvas.draw()

fig1, axs1 = plt.subplots(2, 1)
fig1.suptitle('Figure 1')
fig1.canvas.manager.set_window_title('Figure 1')  # Set window title explicitly

fig1.canvas.mpl_connect('figure_enter_event', on_enter_figure)
fig1.canvas.mpl_connect('figure_leave_event', on_leave_figure)
fig1.canvas.mpl_connect('axes_enter_event', on_enter_axes)
fig1.canvas.mpl_connect('axes_leave_event', on_leave_axes)

fig2, axs2 = plt.subplots(2, 1)
fig2.suptitle('Figure 2')
fig2.canvas.manager.set_window_title('Figure 2')  # Set window title explicitly

fig2.canvas.mpl_connect('figure_enter_event', on_enter_figure)
fig2.canvas.mpl_connect('figure_leave_event', on_leave_figure)
fig2.canvas.mpl_connect('axes_enter_event', on_enter_axes)
fig2.canvas.mpl_connect('axes_leave_event', on_leave_axes)

# Set up a timer to poll for active window every 500ms
timer = fig1.canvas.new_timer(interval=500)
timer.add_callback(check_active_figure)
timer.start()

plt.show()