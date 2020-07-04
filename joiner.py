import json

from splunklib.searchcommands import dispatch, EventingCommand, Option, Configuration, validators


@Configuration()
class Joiner(EventingCommand):
    """
    combines transactions results into one json, assuming each of those individual results is a json
    """
    should_list = Option(
        doc="should generate list instead of overwriting conflicting keys",
        default=False,
        validate=validators.Boolean()
    )

    def transform(self, records):
        """
        applies the appropriate update function to each record
        """
        updating_func = type(self)._update_and_overwrite
        if self.should_list:
            updating_func = self._update_and_list

        for record in records:
            unified_json = {}
            individual_json_list = record['_raw'].split('\n')
            updating_func(unified_json, individual_json_list)
            record['_raw'] = json.dumps(unified_json)
            yield record

    @staticmethod
    def _update_and_overwrite(unified_json, individual_json_list):
        for doc in individual_json_list:
            loaded = json.loads(doc)
            unified_json.update(loaded)  # update function automatically overwrites duplicate keys

    def _update_and_list(self, unified_json, individual_json_list):
        for doc in individual_json_list:
            loaded = json.loads(doc)
            for key, value in loaded.items():
                existing_value = unified_json.get(key)
                if key in self.fieldnames and existing_value:  # where self.fieldnames are keys NOT to list
                    continue
                elif key in self.fieldnames and not existing_value:
                    unified_json[key] = value
                else:
                    type(self)._do_safe_update(unified_json, existing_value, key, value)

    @staticmethod
    def _do_safe_update(unified_json, existing_value, key, value):
        if existing_value:
            unified_json[key].append(value)
        else:
            unified_json[key] = [value]


dispatch(Joiner)
