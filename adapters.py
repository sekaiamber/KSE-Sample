import re


###############
# Adapters
###############

class BaseAdapter(object):

    grok_pattern = r'.+'
    es_prefix = 'debug-logstash-bzfun-'
    es_doc_type = 'web'

    def act(self, source):
        if 'path' not in source:
            return []
        g = self.grok(source['path'])
        if g is None:
            return []
        source = self.pretreat(source, extra=g)
        source = self.build(source)
        return [source]

    def grok(self, path):
        m = re.match(self.grok_pattern, path)
        if m is None:
            return None
        else:
            return m.groupdict()

    def pretreat(self, source, extra={}):
        for key in extra:
            source[key] = extra[key]
        return source

    def build(self, source):
        return source

    def addSpecificParameters(self, source, parms=None):
        ret = {
            '@timestamp': source.get('@timestamp', None),
            'type': self.es_doc_type,
            'path': source.get('path', None)
        }
        if parms is None:
            return ret
        for key in parms:
            ret[key] = None
        spara = source.get('parameters', None)
        if spara is None:
            return ret
        for key in parms:
            ret[key] = spara.get(key, None)
        return ret


"""
grok {
  match => { "path" => "^/ping/sc/?" }
  add_tag => ["searchclick"]
}
"""
class searchClickAdapter(BaseAdapter):

    grok_pattern = r'/ping/sc/?'
    es_prefix = 'debug-logstash-searchclick-'
    es_doc_type = 'searchclick'

    def build(self, source):
        return self.addSpecificParameters(source, [
            'q', 'sid'
        ])


"""
grok {
  match => { "path" => "^/ping/rc/?" }
  add_tag => ["recommendclick"]
}
"""
class recommendclickAdapter(BaseAdapter):

    grok_pattern = r'/ping/rc/?'
    es_prefix = 'debug-logstash-recommendclick-'
    es_doc_type = 'recommendclick'

    def build(self, source):
        return self.addSpecificParameters(source, [
            'user_id', 'series_id', 'sid'
        ])


"""
grok {
  match => { "path" => "^/ping/online/?" }
  add_tag => ["playonline"]
}
"""
class playonlineAdapter(BaseAdapter):

    grok_pattern = r'/ping/online/?'
    es_prefix = 'debug-logstash-playonline-'
    es_doc_type = 'playonline'

    def build(self, source):
        return self.addSpecificParameters(source, [
            'user_id', 'series_id', 'room_id', 'user_action', 'online_count'
        ])


"""
grok {
  match => { "path" => "^/admin/series/(?<series_id>\d+)/(?<flag>add_series_to_top|remove_series_from_top)/?" }
  add_tag => ["stickyseries"]
}
"""
class stickyseriesAdapter(BaseAdapter):

    grok_pattern = r'/admin/series/(?P<series_id>\d+)/(?P<flag>add_series_to_top|remove_series_from_top)/?'
    es_prefix = 'debug-logstash-stickyseries-'
    es_doc_type = 'stickyseries'

    def build(self, source):
        ret = self.addSpecificParameters(source)
        ret['series_id'] = source['series_id']
        ret['series_recommend'] = source['flag'] == 'add_series_to_top'
        return ret


"""
grok {
  match => { "path" => "^/videos/(?<video_id>\d+)/view/?" }
  add_tag => ["viewcount"]
}
"""
class viewcountAdapter(BaseAdapter):

    grok_pattern = r'/videos/(?P<video_id>\d+)/view/?'
    es_prefix = 'debug-logstash-viewcount-'
    es_doc_type = 'viewcount'

    def build(self, source):
        ret = self.addSpecificParameters(source)
        ret['video_id'] = source['video_id']
        return ret
