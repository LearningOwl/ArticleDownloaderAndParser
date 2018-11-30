import newspaper

class SingleSource(newspaper.Source):
    def __init__(self, articleURL):
        super(SingleSource, self).__init__("http://localhost")
        self.articles = [newspaper.Article(url=articleURL)]
