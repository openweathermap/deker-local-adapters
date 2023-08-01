# deker-local-adapters - local filesystem HDF5 storage for deker
# Copyright (C) 2023  OpenWeather
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from dataclasses import asdict, dataclass
from typing import List, Optional, Tuple, Union

from deker.ABC.base_collection import BaseCollectionOptions
from deker.errors import DekerValidationError
from deker.types import Serializer
from deker_local_adapters.storage_adapters.enums import HDF5BuiltinCompressionStringOptions, HDF5ChunksOptions


@dataclass()
class HDF5CompressionOpts(Serializer):
    """HDF5 compression options.

    :param compression: compression filter name
    :param compression_opts: compression level

                             Depending on compressing filter compression_opts may be None, integer or tuple.
                             E.g.:

                             - `hdf5plugin.Blosc` object accepts several parameters, including `clevel`,
                             - `hdf5plugin.Zstd` object accepts an integer in range 1-22,
                             - `szip` filter accepts a tuple of a string and an integer,
                             - `hdf5plugin.FciDecomp` or `lzf` filters do not accept any parameters at all.

                             Read correspondent filter documentation for more information.
    """

    compression: Optional[Union[str, int]]
    compression_opts: Optional[Union[list, tuple, int]]

    def __post_init__(self) -> None:
        """Convert compression options to tuple."""
        if self.compression is not None:
            if not isinstance(self.compression, (str, int)) or isinstance(self.compression, bool):
                raise DekerValidationError(
                    f"Invalid compression type: {type(self.compression)}; str, int or None expected"
                )

            if isinstance(self.compression, str) and (
                self.compression.isspace() or not self.compression
            ):
                raise DekerValidationError(f"Invalid compression value: {self.compression}")

            if isinstance(self.compression, int) and self.compression < 0:
                raise DekerValidationError(f"Invalid compression value: {self.compression}")

        if self.compression_opts is not None:
            if self.compression is None:
                raise DekerValidationError(
                    "HDF5CompressionOpts `compression_opts` is not None and `compression` is None. "
                    "Perhaps you forgot to indicate filter name?"
                )

            if not isinstance(self.compression_opts, (list, tuple, int)) or isinstance(
                self.compression_opts, bool
            ):
                raise DekerValidationError(
                    f"Invalid compression_opts type: {type(self.compression)}; list, tuple, int or None expected"
                )

            if isinstance(self.compression_opts, list):
                self.compression_opts = tuple(self.compression_opts)

            if not self.compression_opts:
                self.compression_opts = None

            if isinstance(self.compression_opts, int) and self.compression_opts < 0:
                raise DekerValidationError(
                    f"Invalid compression level value: {self.compression_opts}"
                )

    @property
    def as_dict(self) -> dict:
        """Serialize self into a dictionary."""
        return {"compression": self.compression, "compression_opts": self.compression_opts}

    def __repr__(self) -> str:
        """Serialize self into a string."""
        return f"{self.__class__.__name__}(compression={self.compression}, compression_opts={self.compression_opts})"

    def __str__(self) -> str:
        """Serialize self into a string."""
        return self.__repr__()


@dataclass()
class HDF5Options(BaseCollectionOptions):
    """Class for collection on-disk configuration.

    Provided that local storage is based on `hdf5`-files, it uses chunks and compression for disk usage optimization.

    :param chunks: Data, stored in hdf5, may be chunked.

                   HDF5-format and `h5py` library provide 3 use cases:

                   1) None: no chunking is used
                   2) True: data is being chunked automatically with the algorithms and in pieces calculated by HDF5
                   3) tuple of integers: user defined chunk-size.
                      E.g. data shape is (100, 100, 100). Upon his/her mind user may chunk it by (1, 10, 100)
                      or by any other applicable chunk size. See chunked storage reference.

    :param compression_opts: HDF5 compression options.

                             HDF5 chunked data may be transformed by the HDF5 filter pipeline.
                             To use built-in and custom compression filters correctly you shall unpack such filter
                             into a HDF5CompressionOpts. There are 3 built-in lossless compression filters:

                             1) "gzip",
                             2) "lzf",
                             3) "szip"

    More filters can be found in `hdf5plugin`.
    ref.: https://docs.h5py.org/en/stable/high/dataset.html#chunked-storage
    ref.: https://docs.h5py.org/en/stable/high/dataset.html#filter-pipeline
    """

    chunks: Optional[Union[bool, Tuple[int, ...], List[int]]] = None
    compression_opts: Optional[HDF5CompressionOpts] = None

    def __post_init__(self) -> None:
        """Validate collection options."""
        if self.chunks is not None:
            if not isinstance(self.chunks, (bool, list, tuple)):
                raise DekerValidationError(
                    f"Invalid chunks type: {type(self.chunks)}; None, True, list or tuple of integers expected"
                )
            if isinstance(self.chunks, bool) and self.chunks is False:
                raise DekerValidationError(
                    f"Invalid chunks type: {type(self.chunks)}; None, True, list or tuple of integers expected"
                )
            if not isinstance(self.chunks, bool):
                if isinstance(self.chunks, list):
                    self.chunks = tuple(self.chunks)

                if (
                    isinstance(self.chunks, tuple)
                    and not self.chunks
                    or not all(isinstance(i, int) for i in self.chunks)
                ):
                    raise DekerValidationError(
                        f"Invalid chunks values: {type(self.chunks)}; None, bool, list or tuple of integers expected"
                    )

        if self.compression_opts is None:
            self.compression_opts = HDF5CompressionOpts(compression=None, compression_opts=None)
        else:
            if not isinstance(self.compression_opts, HDF5CompressionOpts):
                raise DekerValidationError(
                    f"HDF5Options compression_opts invalid type: {type(self.compression_opts)}; "
                    f"`HDF5CompressionOpts` expected"
                )

    @classmethod
    def _process_options(cls, storage_options: Optional[dict]) -> dict:  # noqa: C901
        """Validate and convert options for HDF5.

        :param storage_options: HDF5 compression options and/or chunks.
        """
        coll_params = {}
        compression = {}
        if storage_options:
            for option in storage_options:
                dic = {option: storage_options[option]}
                if option == "compression":
                    if compression_type := dic[option].get("compression"):
                        if dic[option].get("options"):
                            compression_options = []
                            for opt in dic[option]["options"]:
                                try:
                                    compression_option = int(opt)
                                except ValueError:
                                    compression_option = opt
                                compression_options.append(compression_option)
                        else:
                            compression_options = None

                        if compression_type == HDF5BuiltinCompressionStringOptions.none.value:
                            compression_type = None
                            compression_options = None
                        elif compression_type == HDF5BuiltinCompressionStringOptions.gzip.value:
                            if compression_options:
                                compression_options = compression_options[0]
                        elif compression_type == HDF5BuiltinCompressionStringOptions.szip.value:
                            if compression_options:
                                compression_options = tuple(compression_options)
                        elif compression_type == HDF5BuiltinCompressionStringOptions.lzf.value:
                            if compression_options:
                                raise ValueError("LZF filter does not accept any options")
                        else:
                            compression_type = int(compression_type)

                    else:
                        compression_options = None

                    compression.update(
                        {
                            "compression": compression_type,
                            "compression_opts": compression_options,
                        }
                    )
                else:
                    if not dic.get("chunks"):
                        chunks = "none"
                    else:
                        chunks = dic["chunks"]["mode"]

                    if chunks == HDF5ChunksOptions.manual.value:
                        chunks = dic["chunks"]["size"]
                    elif chunks == HDF5ChunksOptions.true.value:
                        chunks = True
                    else:
                        chunks = None
                    coll_params.update({"chunks": chunks})
        if compression:
            coll_params.update({"compression_opts": HDF5CompressionOpts(**compression)})  # type: ignore[dict-item]

        return coll_params

    @property
    def as_dict(self) -> dict:
        """Serialize object as dict."""
        compression_dict = asdict(self.compression_opts)  # type: ignore[arg-type]
        options: Optional[Tuple[Optional[str], ...]] = None

        if self.compression_opts.compression is None:
            compression_dict["compression"] = "none"
            options = tuple()
        else:
            opts = compression_dict["compression_opts"]
            if opts is not None:
                if not isinstance(opts, (list, tuple)):
                    opts = [opts]
                options = tuple(str(opt) for opt in opts)
            compression_dict["compression"] = str(compression_dict["compression"])

        chunks = self.chunks
        if isinstance(self.chunks, (list, tuple)):
            chunks = {"mode": "manual", "size": self.chunks}
        elif chunks:
            chunks = {"mode": "true"}
        else:
            chunks = {"mode": "none"}
        return {
            "chunks": chunks,
            "compression": {"compression": compression_dict["compression"], "options": options},
        }

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(chunks={self.chunks}, compression_opts={self.compression_opts})"

    def __str__(self) -> str:
        return self.__repr__()
