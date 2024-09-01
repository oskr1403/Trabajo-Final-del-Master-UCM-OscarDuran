import xarray as xr

# Ruta al archivo NetCDF
file_path = '/tmp/GSL_C3S-glob-agric_hadgem2-es_rcp2p6_yr_20110101-20401231_v1.1.nc'

# Abrir el dataset y listar las variables
ds = xr.open_dataset(file_path)
print("Variables disponibles en el archivo NetCDF:")
print(ds.variables.keys())
