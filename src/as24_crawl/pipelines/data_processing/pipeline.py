from kedro.pipeline import Pipeline, node, pipeline

from .cleanup import clean_data

from .crawl_nodes import crawl_node


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=crawl_node,
                inputs=["params:base_url", "params:year_range", "params:url_params", "params:countries", "params:brand_model"],
                outputs="crawling_results",
                name="crawl_node",
            ),
            node(
                func=clean_data,
                inputs=["crawling_results"],
                outputs="cleaned_results",
                name="clean_data",
            )
        ]
    )
