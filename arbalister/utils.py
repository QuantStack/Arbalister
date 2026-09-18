def escape(name: str) -> str:
    """Quote a SQL identifier so that it is taken verbatim.

    Unquoted identifiers are lowercased and dots are read as qualifiers.
    """
    escaped = name.replace('"', '""')
    return f'"{escaped}"'
