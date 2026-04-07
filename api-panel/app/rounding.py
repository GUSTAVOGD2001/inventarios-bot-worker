import math


def round_up_x9_99(price: float) -> float:
    """Redondea hacia ARRIBA al próximo X9.99. 165.69 → 169.99"""
    if price <= 9.99:
        return 9.99
    tens = math.floor(price / 10)
    target = tens * 10 + 9.99
    if price > target:
        target += 10
    return round(target, 2)


def round_nearest_99_cents(price: float) -> float:
    """Cambia los centavos al .99 del entero más cercano.
    57.30 → 57.99, 57.60 → 57.99, 58.40 → 58.99, 142.30 → 142.99
    Redondea el entero al más cercano y le pone .99.
    """
    if price < 1:
        return 0.99
    # Redondea al entero más cercano y le pone .99
    nearest_int = round(price)
    return round(nearest_int - 0.01, 2) if nearest_int > 0 else 0.99


def apply_rounding(price: float, threshold: float, low_mode: str, high_mode: str) -> tuple[float, str]:
    """
    Aplica el redondeo adecuado según el rango del precio.
    Retorna (precio_redondeado, modo_aplicado).

    threshold: precio que divide los dos rangos
    low_mode: modo para precios < threshold ('nearest_99', 'ceil_x9_99', 'none')
    high_mode: modo para precios >= threshold ('nearest_99', 'ceil_x9_99', 'none')
    """
    mode = low_mode if price < threshold else high_mode
    if mode == "nearest_99":
        return round_nearest_99_cents(price), "nearest_99"
    elif mode == "ceil_x9_99":
        return round_up_x9_99(price), "ceil_x9_99"
    else:
        return round(price, 2), "none"
