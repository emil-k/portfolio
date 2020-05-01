from django.db import models

# Create your models here.

class Report(models.Model):
    index = models.IntegerField(primary_key=True)
    date = models.DateField()
    channel = models.TextField()
    country = models.TextField()
    os = models.TextField()
    impressions = models.IntegerField()
    clicks = models.IntegerField()
    installs = models.IntegerField()
    spend = models.FloatField()
    revenue = models.FloatField()

    # property decorator lets us use a function name like its a column
    @property
    def cpi(self):
        return self.spend / self.installs

    class Meta:
        db_table = "stats"