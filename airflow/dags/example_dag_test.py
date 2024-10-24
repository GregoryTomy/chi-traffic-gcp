import internal_unit_testing


def test_dag_import():
    from . import 1.0_chi_openData_to_gcs.py

    internal_unit_testing.assert_has_valid_dag(1.0_chi_openData_to_gcs.py)
