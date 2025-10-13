In sepal terminal:


.. code-block:: bash

    # Clone repo
    git clone https://github.com/SerafiniJose/deforisk-jupyter-nb-v2.git

    cd deforisk-jupyter-nb-v2

    pip install uv

    uv run main.py

    uv pip install gdal[numpy]==3.8.4 --no-build-isolation --no-cache-dir --force-reinstall

    source .venv/bin/activate

    python -m ipykernel install --user --name deforisk-deg --display-name "deforisk-deg"







