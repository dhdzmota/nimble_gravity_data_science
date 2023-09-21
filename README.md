Nimble Gravity Challenge: Data Science
--------------
Given the information provided in the `resources` folder, an approach was generated to find the relevant information
through the use of advanced statistical analysis and non-supervised machine learning tools. 

Solution
--------------
Solution to the challenge is in the `notebooks` folder.

Project Organization
------------

    ├── README.md          <- The top-level README file for understanding the project.
    ├── data
    │   ├── raw            <- The original, immutable data dump.
    │   ├── interim        <- Some process was applied to raw data
    │   └── processed      <- Additional process for another data set (from raw).
    │
    ├── figures            <- Folder with the images relevant to the challenge.
    │
    ├── notebooks          <- Folder that contains the Results as jupyter notebooks or related files.
    │   ├── DS-PythonTest(task1_and_task2).ipynb   <- Answer to the task1 and task2 of the challenge.
    │   ├── DS-PythonTest(task3).ipynb             <- Answer to the task3 of the challenge.
    │   ├── DS-PythonTest(task1_and_task2).html    <- HTML representation for task1 and task2.
    │   ├── DS-PythonTest(task3).html              <- HTML representation for task3.
    │   └── README.md                              <- README file.
    │
    ├── python_code        <- Python code for use in this project.
    │   ├── download_s3_data.py          <- File to download all the data.
    │   ├── process_data_to_interim.py   <- File to read raw data process it and send it into interim folder.
    │   └── process_data_to_processed.py <- File to read raw data process it and send it into processed folder.
    │
    ├── resources          <- Information and documents relevant to the challenge.
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    └── config.yaml       <-  Yaml file that contains credentials. 

Additional Considerations
------------

This project is executed with `Python 3.8.10`. 