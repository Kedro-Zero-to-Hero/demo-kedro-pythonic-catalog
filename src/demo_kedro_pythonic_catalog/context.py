from typing import Dict
from kedro.framework.context import KedroContext
from kedro.framework.context.context import (
    KedroContextError,
    _convert_paths_to_absolute_posix,
    _validate_layers_for_transcoding
)
from kedro.framework.hooks import get_hook_manager
from kedro.io.data_catalog import DataCatalog
from kedro.versioning.journal import Journal


class ProjectContext(KedroContext):

    def _get_catalog(
        self,
        save_version: str = None,
        journal: Journal = None,
        load_versions: Dict[str, str] = None,
    ) -> DataCatalog:
        """A hook for changing the creation of a DataCatalog instance.

        Returns:
            DataCatalog defined in `catalog.yml`.
        Raises:
            KedroContextError: Incorrect ``DataCatalog`` registered for the project.

        """
        # '**/catalog*' reads modular pipeline configs
        conf_catalog = self.config_loader.get("catalog*", "catalog*/**", "**/catalog*")
        # turn relative paths in conf_catalog into absolute paths
        # before initializing the catalog
        conf_catalog = _convert_paths_to_absolute_posix(
            project_path=self.project_path, conf_dictionary=conf_catalog
        )
        conf_creds = self._get_config_credentials()

        hook_manager = get_hook_manager()
        catalog = hook_manager.hook.register_catalog(  # pylint: disable=no-member
            catalog=conf_catalog,
            credentials=conf_creds,
            load_versions=load_versions,
            save_version=save_version,
            journal=journal,
        )
        if not isinstance(catalog, DataCatalog):
            raise KedroContextError(
                f"Expected an instance of `DataCatalog`, "
                f"got `{type(catalog).__name__}` instead."
            )

        feed_dict = self._get_feed_dict()
        catalog.add_feed_dict(feed_dict)
        if catalog.layers:
            _validate_layers_for_transcoding(catalog)
        hook_manager = get_hook_manager()
        hook_manager.hook.after_catalog_created(  # pylint: disable=no-member
            catalog=catalog,
            conf_catalog=conf_catalog,
            conf_creds=conf_creds,
            feed_dict=feed_dict,
            save_version=save_version,
            load_versions=load_versions,
            run_id=self.run_id,
        )
        return catalog
