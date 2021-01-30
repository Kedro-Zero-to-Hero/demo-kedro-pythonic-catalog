from kedro.extras.datasets import pandas


class ExampleIrisData:
    name = "example_iris_data"
    type = pandas.CSVDataSet
    filepath = "data/01_raw/iris.csv"


CATALOG = [ExampleIrisData]


for country in ["uk", "us", "au", "fr", "es"]:
    dataset_class_name = f"{country.upper()}ExampleIrisData"
    dataset_name = f"{country}_example_iris_data"
    dataset_def = type(
        dataset_class_name,
        (),
        {
            "name": dataset_name,
            "type": pandas.CSVDataSet,
            "filepath": f"data/01_raw/{country}/iris.csv",
        },
    )
    CATALOG.append(dataset_def)
