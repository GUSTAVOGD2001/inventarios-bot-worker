import math


def round_up_x9_99(price: float) -> float:
    """
    Redondea hacia ARRIBA al próximo X9.99
    142.30 -> 149.99 | 149.99 -> 149.99 | 150.00 -> 159.99
    165.69 -> 169.99 | 198.01 -> 199.99 | 9.50 -> 9.99
    """
    if price <= 9.99:
        return 9.99
    tens = math.floor(price / 10)
    target = tens * 10 + 9.99
    if price > target:
        target += 10
    return round(target, 2)
