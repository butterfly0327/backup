INSERT INTO dataset_source (source_code, source_name, base_url, collection_type, is_active)
VALUES
    ('HUGGINGFACE', 'Hugging Face', 'https://huggingface.co', 'API', TRUE),
    ('PUBLIC_DATA_PORTAL', '공공데이터포털', 'https://www.data.go.kr', 'CRAWL', TRUE),
    ('FIGSHARE', 'Figshare', 'https://api.figshare.com/v2', 'API', TRUE),
    ('HARVARD_DATAVERSE', 'Harvard Dataverse', 'https://dataverse.harvard.edu', 'API', TRUE),
    ('KAGGLE', 'Kaggle', 'https://www.kaggle.com', 'API', TRUE),
    ('AI_HUB', 'AI Hub', 'https://www.aihub.or.kr', 'CRAWL', TRUE),
    ('AWS_ODR', 'AWS Open Data Registry', 'https://registry.opendata.aws', 'API', TRUE),
    ('ZENODO', 'Zenodo', 'https://zenodo.org/api', 'API', TRUE),
    ('DATA_GOV', 'data.gov catalog', 'https://api.gsa.gov/technology/datagov/v3', 'API', TRUE),
    ('DATA_EUROPA', 'data.europa.eu', 'https://data.europa.eu', 'API', TRUE)
ON CONFLICT (source_code)
DO UPDATE SET
    source_name = EXCLUDED.source_name,
    base_url = EXCLUDED.base_url,
    collection_type = EXCLUDED.collection_type,
    is_active = TRUE;
