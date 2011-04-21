import datetime

from gooddataclient.dataset import Dataset
from gooddataclient import columns

from django.contrib.auth.models import User as DjangoUser
from django.conf import settings


class User(Dataset):

    id = columns.ConnectionPoint(title='Id', folder='User')
    date_joined = columns.Date(title='Date Joined', folder='User', format='yyyy-MM-dd', schemaReference='Visit')
    is_active = columns.Fact(title='Is Active', folder='User')

    class Meta(Dataset.Meta):
        project_name = settings.GOODDATA_GOOGLE_ANALYTICS_PROJECT

    def data(self):
        return list(DjangoUser.objects.filter(date_joined__gt=datetime.date(2010, 12, 1)).values('id', 'date_joined', 'is_active'))

