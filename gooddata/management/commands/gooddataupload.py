import inspect
from optparse import make_option

from gooddataclient.connection import Connection
from gooddataclient.project import Project
from gooddataclient.dataset import Dataset
from gooddata import logger

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.importlib import import_module
from django.utils.module_loading import module_has_submodule


def discover_gooddata_modules():
    gd_classes = {}
    for app in settings.INSTALLED_APPS:
        mod = import_module(app)
        try:
            gd_module = import_module('%s.gooddata' % app)
            for (cls_name, cls) in inspect.getmembers(gd_module, inspect.isclass):
                if not issubclass(cls, Dataset):
                    continue
                app_cls_name = '.'.join((app.split('.')[-1], cls_name))
                gd_classes[app_cls_name] = cls
        except:
            if module_has_submodule(mod, 'gooddata'):
                raise
    return gd_classes


class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option('-d', '--datasets', help='Select Data Sets for uploading to GoodData in format app_name.ClassName'),
    )

    def handle(self, **options):
        '''GoodData upload command'''
        use_datasets = options.get('datasets').split(',') if options.get('datasets') else None

        gd_classes = discover_gooddata_modules()
        if use_datasets:
            for app_cls_name in gd_classes.keys():
                if app_cls_name not in use_datasets:
                    del gd_classes[app_cls_name]

        connection = Connection(settings.GOODDATA_USERNAME, settings.GOODDATA_PASSWORD)
        projects = {}
        for dataset_class in gd_classes.itervalues():
            if not dataset_class.project_name:
                raise AttributeError('project_name not defined in %s' % dataset_class)
            if dataset_class.project_name not in projects:
                projects[dataset_class.project_name] = Project(connection).load(name=dataset_class.project_name)
            dataset = dataset_class(projects[dataset_class.project_name])
            dataset.upload()

            #altering existing structure is not needed yet, so not implemented
            #see http://developer.gooddata.com/api/maql-ddl.html for specification

