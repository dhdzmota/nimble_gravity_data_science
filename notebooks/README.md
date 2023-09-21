Nimble Gravity Challenge: Data Science
--------------

This folder contains the results of the Nimble Gravity Challenge for the Data Science step.
Please feel free to open the `.ipynb` or the `.html` files to see the solution. 

Task1 and Task2 are resolved in the `DS-PythonTest(task1_and_task2).ipynb` file.
Task3 is resolved in the `DS-PythonTest(task3).ipynb` file.

If you desire to execute the notebooks, first all the data must be downloaded and processed.
To do this follow the following steps:

- Make sure to have a `config.yaml` file at the root of this repo with 
the necessary credentials (take the `confige.yaml.default` as example to modify without changing the keys).
- Make sure you have a virtual environment to pip install the requirements.txt file by running `pip install -r requirements.txt`
- Run the following python scripts in the provided order that are found in the `python_code` folder:
  - `download_s3_data.py`
  - `process_data_to_interim.py`
  - `process_data_to_processed.py`
- Now you can run the notebooks.