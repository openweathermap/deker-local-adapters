Deker local adapters
====================

Deker local adapters provide file storage support for Deker, enabling the storage and management of numeric data on the local file system.
The module supports storage in the HDF5 file format and allows configuring HDF chunks and compression.

Modules
-------

- **Collection Adapter**: Manages collection objects on the file system level.
- **Array Adapter**: Manages Array objects, provides CRUD methods, allows to update data and meta.
- **VArray Adapter**: Manages VArray objects.
- **HDF5 Options**: Provides options for configuring how collections save arrays in HDF5 format.
- **HDF5 Storage Adapter**: Handles the actual storage and retrieval of data in HDF5 format.

.. toctree::
   :maxdepth: 4
   :hidden:

    Deker local adapters API <source/api/modules>
