from importlib import import_module


def get_class(class_path):
    module_name = class_path.rpartition(".")[0]
    class_name = class_path.rpartition(".")[-1]
    module = import_module(module_name)
    return getattr(module, class_name)
