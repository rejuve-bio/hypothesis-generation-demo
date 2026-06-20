import marimo

__generated_with = "0.7.0"
app = marimo.App(width="medium")


@app.cell
def __(mo):
    mo.md(r"""
    # deltaSVM Pipeline
    End-to-end pipeline for scoring SNP allelic effects on TF binding using deltaSVM.
    """)
    return


if __name__ == "__main__":
    app.run()
