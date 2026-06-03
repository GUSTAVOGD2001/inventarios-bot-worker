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
    """Pone los centavos en .99 del MISMO entero, sin bajar.
    Ejemplos:
        56.00 → 56.99
        56.30 → 56.99
        56.99 → 56.99 (ya está)
        57.01 → 57.99
        142.30 → 142.99
    """
    if price <= 0.99:
        return 0.99
    integer_part = math.floor(price)
    candidate = integer_part + 0.99
    return round(candidate, 2)


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
