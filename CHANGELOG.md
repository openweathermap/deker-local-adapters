# Changelog

All notable changes to deker-local-adapters, the local HDF5 storage of [DEKER™](https://github.com/openweathermap/deker),
are listed here, release by release, newest first. Only production releases are listed. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), the versions follow
[Semantic Versioning](https://semver.org/).

## [2.0.0]

Released together with DEKER™ 2.0.0; requires `deker>=2,<3` and `deker-tools>=2,<3`. The locking
protocol changed, so every process working with one storage must be updated at once.

### Breaking changes

- Locks without marker files: a reader holds a shared lock on the array file for the whole read,
  a writer and a deletion take an exclusive lock and wait for the readers to leave. A VArray is
  written Array by Array. The marker files of 1.x are not created and not honoured; the locks do
  not work over NFS.
- Creating an array is atomic. The directory of its symlink (it is defined by the primary
  attributes) is locked for the whole creation, the HDF5 file is created in the `w-` mode and the
  metadata file in the `x` mode: a second creation of an array with the same primary attributes
  raises `DekerValidationError`, and an existing file is never overwritten. The lock is on the
  directory itself, no extra files appear. `check_array_file_existence` is removed.
- Deleting an array waits for its readers and writers, like a write does.
- `update()` with `None` raises `DekerArrayError` instead of `ValueError`.
- The storage adapter interface requires three new methods for the `counters` integrity check:
  `read_empty_cells`, `count_empty_cells` and `write_empty_cells`; the HDF5 adapter implements them.
- The adapter interfaces are generic over the path and the array type, and `create()` returns the
  created array (`deker` 2.0.0).

### Changed

- New files are created with the HDF5 free-space tracking on (`fs_strategy=fsm`,
  `fs_persist=True`): the space freed by an update of the attributes is reused and the file does
  not grow with every update. The files of 1.x are not converted; the files of 2.0.0 are read by
  1.x.
- The first write into an array creates the data with the declared fill value and writes only the
  given region, instead of building the whole array in memory and writing it entirely. The
  unwritten parts take no disk space.
- Reading from an array that was never written builds only the requested region, not the whole
  array.
- A write that covers the whole array does not read it first; a write of nothing but the fill
  value over the whole array removes the data instead of writing them.
- The metadata block is sized by the encoded length and holds UTF-8 bytes: 33 bytes less per file,
  and attributes with non-ASCII characters work.
- An update of the custom attributes of a `VArray` writes the new metadata next to the json file and
  moves it over in one step: a reader never meets a half-written file. The lock of the update and of
  the creation of the Arrays is on the directory of the file, which stays, not on the file.
- Deleting a collection raises when its directory cannot be removed, instead of swallowing the error; a
  directory that is gone already is not an error.
- The errors of the adapters go to the DEKER™ logger, not to the root logger.

### Fixed

- The empty-cell counter of a file drifted: every write decreased it by the size of the region,
  even when the region was written before. It is now computed from the data: how many empty cells
  the region had before the write and how many it has after. An old wrong counter is reported and
  fixed by the `counters` integrity check of DEKER™.
- Clearing a part of an array could delete all its data: the decision was taken by the counter.
  The data are removed only when every cell is empty, and that is checked against the data.

### Internals and tooling

- The build moved from poetry to uv, ruff and pydoclint replace black, flake8, isort and darglint;
  `make ci`, `make test-matrix`, `make install-matrix`; `py.typed`; 100% test coverage required;
  `pytest-timeout`; tests in random order.

## [1.2.0] - 2026-11-11

### Added

- Python 3.13 and 3.14 are supported.
- NumPy 2 is supported (`numpy>=1.26,<3`).

### Changed

- `h5py>=3.11,<4` (was `^3.8`), `hdf5plugin>=4.0.1,<8` (was `^4`); the other dependency ranges
  are widened to the current majors; the dev and docs tooling is updated.
- CI runs the tests on every supported Python.

### Removed

- Python 3.9. Pin `deker-local-adapters<1.2` to stay on it.

## [1.1.2] - 2024-07-03

### Changed

- The memory check of a collection creation is skipped when the client is created with
  `skip_collection_create_memory_check=True` (`deker>=1.1.7`).
- `HDF5_USE_FILE_LOCKING` is switched back to `FALSE`.

### Fixed

- Deleting an array removes the data file the symlink points to, then the symlink, then the
  empty parent directories of both; before, only the symlink side was cleaned.

## [1.1.1] - 2024-05-08

### Changed

- `LocalArrayAdapter.create` and `LocalVArrayAdapter.create` take no lock and return nothing
  anymore (`CreateArrayLock` of DEKER™ is removed).

## [1.1.0] - 2023-12-28

### Added

- `DekerBrokenSymlinkError` is raised when a symlink points to a file that does not exist, instead
  of a silent wrong result.

### Changed

- `deker` and `deker-tools` version requirements updated.

## [1.0.1] - 2023-08-18

### Changed

- `BaseArray._create_from_meta` replaces `create_array_from_meta`.
- All the configuration lives in `pyproject.toml`; the CI workflows moved to deker-actions; the
  README is rewritten.

## [1.0.0] - 2023-08-02

The pilot release of deker-local-adapters.

[2.0.0]: https://github.com/openweathermap/deker-local-adapters/compare/v1.2.0...v2.0.0
[1.2.0]: https://github.com/openweathermap/deker-local-adapters/compare/v1.1.2...v1.2.0
[1.1.2]: https://github.com/openweathermap/deker-local-adapters/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/openweathermap/deker-local-adapters/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/openweathermap/deker-local-adapters/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/openweathermap/deker-local-adapters/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/openweathermap/deker-local-adapters/releases/tag/v1.0.0
