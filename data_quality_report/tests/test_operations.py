from data_quality_report.operations import *
from data_quality_report.spark import get_spark


class TestMission(object):

    def test_get_missing_df(self):
        # sample test of one of the stats methods
        source_data = [
            (0.5, 12.3, 54.2),
            (None, 12.3, 54.2),
            (54,3, None, 54.2),
        ]

        source_df = get_spark("test").createDataFrame(
            source_data,
            ["dec1", "dec2", "dec3"]
        )

        actual_df = get_missing_df(source_df)

        expected_data = [
            (1, 1, 0),
        ]
        expected_df = get_spark("test").createDataFrame(
            expected_data,
            ["dec1", "dec2", "dec3"]
        )

        assert (expected_df.collect() == actual_df.collect())

    # the other methods would be tested also here
