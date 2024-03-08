from enum import auto, Flag

class PublishFormat(Flag):
    All = auto()
    Any = auto()
    Single = auto()

# TODO: hasattr() will be necessary when the DriverAgentConfig is Pydantic based and not a dict.
# TODO: Can this be simplified if publish_type flags are not needed in agent-level?
def setup_publishes(config, parent_config=None):
    if not hasattr(config, 'config_version'):
        config.config_version = parent_config.config_version if hasattr(parent_config, 'config_version') else 1
    config_version = config.config_version
    # NOTE: The agent configurations have defaults for all parameters, whereas the controller configs do not.
    #  This will prevent any parameter from being None if there is not a parent_config.
    breadth_first_all = config.publish_breadth_first_all if config.publish_breadth_first_all is not None \
        else PublishFormat.All in parent_config.breadth_first_publishes
    depth_first_all = config.publish_depth_first_all if config.publish_depth_first_all is not None \
        else PublishFormat.All in parent_config.depth_first_publishes
    
    if config_version == 1:
        breadth_first_single = config.publish_breadth_first if config.publish_breadth_first is not None \
            else PublishFormat.Single in parent_config.breadth_first_publishes
        depth_first_single = config.publish_depth_first if config.publish_depth_first is not None \
            else PublishFormat.Single in parent_config.depth_first_publishes

        breadth_first_publishes = PublishFormat(
            PublishFormat.All.value * breadth_first_all
            + PublishFormat.Single.value * breadth_first_single
        )
        depth_first_publishes = PublishFormat(
            PublishFormat.All.value * depth_first_all
            + PublishFormat.Single.value * depth_first_single
        )
    else:  # Version 2 or higher.
        breadth_first_single = config.publish_breadth_first_single if config.publish_breadth_first_single is not None \
            else PublishFormat.Single in parent_config.breadth_first_publishes
        depth_first_single = config.publish_depth_first_single if config.publish_depth_first_single is not None \
            else PublishFormat.Single in parent_config.depth_first_publishes
        breadth_first_any = config.publish_breadth_first_any if config.publish_breadth_first_any is not None \
            else PublishFormat.Any in parent_config.breadth_first_publishes
        depth_first_any = config.publish_depth_first_any if config.publish_depth_first_any is not None \
            else PublishFormat.Any in parent_config.depth_first_publishes

        breadth_first_publishes = PublishFormat(
            PublishFormat.All.value * breadth_first_all
            + PublishFormat.Single.value * breadth_first_single
            + PublishFormat.Any.value * breadth_first_any

        )
        depth_first_publishes = PublishFormat(
            PublishFormat.All.value * depth_first_all
            + PublishFormat.Single.value * depth_first_single
            + PublishFormat.Any.value * depth_first_any
        )
    return breadth_first_publishes, depth_first_publishes
