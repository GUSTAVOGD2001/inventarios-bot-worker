import pytest

from app.rounding import round_up_x9_99


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
