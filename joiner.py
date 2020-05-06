import json

from splunklib.searchcommands import dispatch, EventingCommand, Configuration


@Configuration()
class Joiner(EventingCommand):
    """
    combines transactions results into one json, assuming each of those individual results is a json
    """

    def transform(self, records):
        for record in records:
            individual_json_list = record['_raw'].split('\n')
            final_json = {}

            for doc in individual_json_list:
                loaded = json.loads(doc)
                final_json.update(loaded)

            yield {'_raw': json.dumps(final_json)}


dispatch(Joiner)
