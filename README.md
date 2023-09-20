Nimble Gravity Challenge: Data Science
--------------
Given the information provided in the `resources` folder, an approach was generated to find the relevant information
through the use of advanced statitical analysis and non-supervised machine learning tools. 


Project Organization
------------

    ├── README.md          <- The top-level README file for understanding the project.
    ├── data
    │   ├── raw            <- The original, immutable data dump.
    │   ├── interim        <- Some process was applied to raw data
    │   └── processed      <- Additional process for another data set (from raw).
    │
    ├── figures            <- folder with the images of the results of the challenge.
    │
    ├── notebooks          <- folder that contains the jupyter notebooks to explain with additional detail the challenge.
    │
    ├── python_code        <- Python code for use in this project.
    │   ├── download_s3_data.py    <- File to download all the data.
    │   └── process_data_to_interim.py    <- File to read raw data process it and send it into interim folder.
    │
    ├── resources          <- Information and documents relevant to the challenge.
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │                  
    ├── config.yaml        <- Yaml file that contains credentials.
    │
    └── main_file.sh       <- Main file that contains all the commands to solve the challenge. 

Additional Considerations
------------

This project is executed with `Python 3.8.10`. 