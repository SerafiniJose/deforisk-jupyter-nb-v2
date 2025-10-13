import re


def extract_raw_variables(formula: str) -> set:
    """
    Extract raw variable names from a Patsy-style formula,
    safely handling I(), scale(), C(), and other transformations.

    Example:
        "I(1 - fcc) + trial ~ scale(altitude) + C(pa)"
        → returns {'fcc', 'trial', 'altitude', 'pa'}
    """
    raw_vars = set()

    # Pattern to match: any Patsy function with content inside parentheses
    # We capture the inner part, then extract variables from it
    pattern = r"[a-zA-Z_][a-zA-Z0-9_]*\(([^)]+)\)"

    # Find all expressions like I(...), scale(...), C(...)
    matches = re.findall(pattern, formula)

    for expr in matches:
        # Clean the expression: remove spaces, split by operators
        # We want to extract only variable names (no constants or math)
        tokens = re.split(r"[+\-*/\(\)\s]", expr)  # Split on common symbols
        tokens = [t.strip() for t in tokens if t.strip()]

        # Keep only valid identifiers that are not numbers/strings
        for token in tokens:
            # Skip numeric literals (e.g., '1', '2.3')
            if re.match(r"^\d+(\.\d+)?$", token):
                continue
            # Skip keywords like 'I', 'scale'
            if token.lower() in {"i", "scale", "c", "poly", "bs", "cr"}:
                continue
            raw_vars.add(token)

    # Now extract standalone variables (not inside functions)
    standalone = re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", formula)

    for var in standalone:
        if var.lower() not in {"i", "scale", "c"}:  # Skip Patsy keywords
            raw_vars.add(var)

    # Remove invalid tokens (e.g., '1-fcc' is not a column name)
    raw_vars = {v for v in raw_vars if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v)}

    return raw_vars
