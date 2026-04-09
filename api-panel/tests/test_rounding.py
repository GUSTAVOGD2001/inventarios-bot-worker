import pytest

from app.rounding import round_up_x9_99, round_nearest_99_cents


@pytest.mark.parametrize(
    "price, expected",
    [
        (142.30, 149.99),
        (149.99, 149.99),
        (150.00, 159.99),
        (165.69, 169.99),
        (198.01, 199.99),
        (9.50, 9.99),
        (200.00, 209.99),
        (10.00, 19.99),
        (0.50, 9.99),
        (99.99, 99.99),
        (100.00, 109.99),
        (1.00, 9.99),
        (259.99, 259.99),
        (260.01, 269.99),
    ],
)
def test_round_up_x9_99(price, expected):
    assert round_up_x9_99(price) == expected


@pytest.mark.parametrize(
    "price, expected",
    [
        (56.00, 56.99),
        (56.30, 56.99),
        (56.99, 56.99),
        (57.01, 57.99),
        (57.30, 57.99),
        (57.60, 57.99),  # NO sube a 58.99
        (58.40, 58.99),
        (142.30, 142.99),
        (199.99, 199.99),
        (1.00, 1.99),
        (0.50, 0.99),
        (0.01, 0.99),
    ],
)
def test_round_nearest_99_cents(price, expected):
    assert round_nearest_99_cents(price) == expected


def test_user_reported_case_56_30_should_be_56_99():
    """Caso real reportado: 56.30 debe convertirse en 56.99, no en 55.99"""
    assert round_nearest_99_cents(56.30) == 56.99
