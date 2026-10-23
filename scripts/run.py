from pathlib import Path
import runpy


PIPELINE = (
    Path(__file__).resolve().parents[1]
    / "notebooks"
    / "drosophila_female_lifespan_finemapping_pipeline.py"
)


if __name__ == "__main__":
    runpy.run_path(str(PIPELINE), run_name="__main__")
