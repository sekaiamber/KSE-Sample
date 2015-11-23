import re
from datetime import datetime


###############
# Adapters
###############
class BaseAdapter(object):

    grok_pattern = r'.+'
    es_prefix = 'debug-logstash-bzfun-'
    es_doc_type = 'web'

    @classmethod
    def act(cls, source):
        if 'path' not in source:
            return []
        g = cls.grok(source['path'])
        if g is None:
            return []
        source = cls.pretreat(source, extra=g)
        source = cls.build(source)
        return [source]

    @classmethod
    def grok(cls, path):
        m = re.match(cls.grok_pattern, path)
        if m is None:
            return None
        else:
            return m.groupdict()

    @classmethod
    def pretreat(cls, source, extra={}):
        for key in extra:
            source[key] = extra[key]
        return source

    @classmethod
    def build(cls, source):
        return source

    @classmethod
    def addSpecificParameters(cls, source, parms=None):
        ret = {
            '@timestamp': source.get('@timestamp', None),
            'type': cls.es_doc_type,
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

    @classmethod
    def build(cls, source):
        return cls.addSpecificParameters(source, [
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

    @classmethod
    def build(cls, source):
        return cls.addSpecificParameters(source, [
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

    @classmethod
    def build(cls, source):
        ret = cls.addSpecificParameters(source, [
            'user_id', 'series_ids', 'room_id', 'user_action', 'online_count'
        ])
        si = ret.pop('series_ids')
        ret['series_id'] = si
        return ret


"""
grok {
  match => { "path" => "^/admin/categories/(?<categories_id>\d+)/(?<flag>add_series_to_top|remove_series_from_top)/?" }
  add_tag => ["stickyseries"]
}
"""
class stickyseriesAdapter(BaseAdapter):

    grok_pattern = r'/admin/categories/(?P<categories_id>\d+)/(?P<flag>add_series_to_top|remove_series_from_top)/?'
    es_prefix = 'debug-logstash-stickyseries-'
    es_doc_type = 'stickyseries'

    @classmethod
    def build(cls, source):
        ret = cls.addSpecificParameters(source, [
            'series_id'
        ])
        ret['categories_id'] = source['categories_id']
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

    @classmethod
    def build(cls, source):
        ret = cls.addSpecificParameters(source)
        ret['video_id'] = source['video_id']
        return ret


###############
# Helper
###############
class AdapterHelper(object):

    act_adapters = [
        BaseAdapter,
        searchClickAdapter,
        recommendclickAdapter,
        playonlineAdapter,
        stickyseriesAdapter,
        viewcountAdapter
    ]

    @classmethod
    def getAdapters(cls):
        for adp in cls.act_adapters:
            yield adp

    @classmethod
    def getAdaptersEsResource(cls):
        dt = datetime.now().strftime('%Y.%m.%d')
        for adp in cls.act_adapters:
            yield ("%s/%s" % (
                adp.es_prefix + dt,
                adp.es_doc_type
            ), adp.es_doc_type)
