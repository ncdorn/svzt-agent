"""Public package metadata for svztagent."""

from pydantic import VERSION as PYDANTIC_VERSION

__all__ = ["__version__"]
__version__ = "0.1.0"

if int(PYDANTIC_VERSION.split(".", maxsplit=1)[0]) < 2:
    raise RuntimeError(
        "svzt-agent requires pydantic>=2.6,<3 at runtime. "
        f"Detected pydantic=={PYDANTIC_VERSION}."
    )
