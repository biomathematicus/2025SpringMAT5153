import matplotlib.pyplot as plt

def generate_histogram_from_query(db, query, bins=30, title=None, xlabel=None, ylabel=None):
    """
    1) Execute `query` via DatabaseClient `db` (must return one numeric column).
    2) Fetch the first column of each row (skipping NULLs), cast to float.
    3) Plot a histogram of those values.
    """
    rows = db.query(query)
    data = [float(r[0]) for r in rows if r[0] is not None]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(data, bins=bins, alpha=0.7, edgecolor='black')
    if title:  ax.set_title(title)
    if xlabel: ax.set_xlabel(xlabel)
    if ylabel: ax.set_ylabel(ylabel)
    plt.tight_layout()
    plt.show()
