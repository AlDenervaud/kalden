import numpy as np
import plotly.colors as pc

def save_plotly_fig(fig, filepath, width=1200, height=600, **kwargs):
    """
    Save a Plotly figure to a PNG file.
    
    Args:
        fig: Plotly Figure object or dict.
        filepath (str): Path to save the PNG file (e.g., 'plot.png').
        **kwargs: Additional arguments for fig.write_image(), such as:
            - width (int): Image width in pixels.
            - height (int): Image height in pixels.
            - scale (float): Resolution scale factor (>1 for higher DPI).
            - format (str): Image format ('png', 'jpeg', etc.).
            - engine (str): Export engine ('kaleido' or deprecated 'orca').
    
    Example:
        fig = go.Figure(go.Scatter(x=[1,2], y=[1,2]))
        save_plotly_to_png(fig, 'myplot.png', width=800, height=600, scale=2)
    """
    fig.write_image(filepath, width=width, height=height, **kwargs)
    print(f"Figure saved to {filepath}")


def heatmap_colorscale(values, plotly_colorscale="RdBu_r", zmin=None, zmax=None):
    """
    Take a Plotly named colorscale and shift its midpoint so that the
    center color lands at the position of 0 between zmin and zmax.

    Parameters
    ----------
    values : array-like
        Data values, only used if zmin/zmax are not provided.
    plotly_colorscale : str or list
        Plotly named colorscale, e.g. "RdBu_r", "BrBG", "PuOr".
    zmin, zmax : float or None
        Optional explicit bounds.

    Returns
    -------
    colorscale, zmin, zmax
    """
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]

    if arr.size == 0 and (zmin is None or zmax is None):
        raise ValueError("`values` contains no finite values, and zmin/zmax were not provided.")

    zmin = float(arr.min()) if zmin is None else float(zmin)
    zmax = float(arr.max()) if zmax is None else float(zmax)

    if zmin >= zmax:
        raise ValueError(f"zmin must be < zmax, got zmin={zmin}, zmax={zmax}")

    if not (zmin <= 0 <= zmax):
        raise ValueError(f"Zero must lie within [zmin, zmax], got zmin={zmin}, zmax={zmax}")

    zero_pos = (0.0 - zmin) / (zmax - zmin)

    # Get the original Plotly colorscale as [[pos, color], ...]
    base = pc.get_colorscale(plotly_colorscale)

    shifted = []
    for p, c in base:
        p = float(p)

        if p <= 0.5:
            # map [0, 0.5] -> [0, zero_pos]
            new_p = 0.0 if zero_pos == 0 else (p / 0.5) * zero_pos
        else:
            # map [0.5, 1] -> [zero_pos, 1]
            new_p = 1.0 if zero_pos == 1 else zero_pos + ((p - 0.5) / 0.5) * (1.0 - zero_pos)

        shifted.append([new_p, c])

    return shifted, zmin, zmax
