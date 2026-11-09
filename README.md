# Milan traffic forecasting

Forecasting Internet, SMS and call activity for Milan grid 5060.

## Data

Download from [Harvard Dataverse](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/EGZHFV) using Python 3.11+ on Linux or macOS:

```bash
python3 download_dataset.py --workers 8
```

Saves `data/grid5060_rows.tsv`. Rerunning resumes from verified days. The initial transfer is about 21 GB; only the selected grid is stored. Adjust `--workers` for your connection, or add `--full` to keep all daily files.

## Notebook

Open `notebook.ipynb` after downloading. Use the Jupyter environment specified in `requirements.txt`, then install its extra packages as described there.
