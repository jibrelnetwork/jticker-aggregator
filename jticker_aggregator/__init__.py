import warnings

# Ignore Pandas/Numpy is not available.
# Support for 'dataframe' mode is disabled.
warnings.filterwarnings(
    action='ignore',
    category=UserWarning,
    module="aioinflux.compat",
    lineno=11
)
warnings.filterwarnings(
    action='ignore',
    category=UserWarning,
    module="aioinflux.serialization.common",
    lineno=18
)
