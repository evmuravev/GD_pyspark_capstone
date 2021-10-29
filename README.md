# Repository with a Capstone project of the Capstone Project (Python Engineer to BigData Engineer)

This repository contains the following files:

```
├── result                                  <- the results saved as parquet files
│   ├── purchases_attribution.parquet           
│   ├── top_channels_in_campaign.parquet        
│   └── top_ten_campaign.parquet                
│
├── test                                    
│   ├── test_dataset/...                    <- folder with data for unit tests
│   └── test_main.py                        <- unit tests
│ 
├── conftest.py                             <- fixtures for unit tests
├── main.py                                 <- main script for the spark driver
├── schemas.py                              <- target schemas for spark dataframes
└── README.md
```


## Usage

Clone the repository to your local machine. cd to the directory with the repository.
To get started with the project, make sure you have installed and configured PySpark and then run the following command:
```shell
spark-submit main.py $YOUR_PATH/capstone-dataset/mobile_app_clickstream $YOUR_PATH/capstone-dataset/user_purchases

```

### Notes

PySpark version 3.1.2 running in Local mode

