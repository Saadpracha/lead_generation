from scrapy.exporters import CsvItemExporter

class Utf8BomCsvItemExporter(CsvItemExporter):
    def __init__(self, file, **kwargs):
        kwargs['encoding'] = 'utf-8-sig'  # Adds UTF-8 BOM
        super().__init__(file, **kwargs)
