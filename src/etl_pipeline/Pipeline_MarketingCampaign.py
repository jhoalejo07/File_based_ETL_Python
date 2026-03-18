from src.etl_pipeline.extract.extract import Extract
from src.etl_pipeline.transform.transform_MarketingCampaign import Transform
from src.etl_pipeline.load.load_code import Load


def run():
    """
    Orchestrates the ETL pipeline for Marketing Campaign dataset.
    Executes Extract → Transform → Load sequentially.
    """

    # Extract raw CSV files from data/raw directory
    raw_data = Extract("tira_campaign_data.csv", "nykaa_campaign_data.csv","purplle_campaign_data.csv")

    # Apply business transformation logic
    processed_data = Transform(raw_data.extract())

    # Persist final dataset with versioning
    Load(processed_data)


if __name__ == "__main__":
    run()
