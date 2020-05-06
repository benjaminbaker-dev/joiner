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

    id_key = Option(
        doc='''
        if should list is set, this parameter enables you to select the id 
        field the transaction was done on, so to not create a list
        ''',
        default=""
    )

    def transform(self, records):
        """
        applies the appropriate update function to each record
        """
        final_json = {}
        updating_func = type(self)._update_and_overwrite
        if self.should_list:
            updating_func = self._update_and_list

        for record in records:
            individual_json_list = record['_raw'].split('\n')
            updating_func(final_json, individual_json_list)
            yield {'_raw': json.dumps(final_json)}

    @staticmethod
    def _update_and_overwrite(final_json, individual_json_list):
        for doc in individual_json_list:
            loaded = json.loads(doc)
            final_json.update(loaded)

    def _update_and_list(self, final_json, individual_json_list):
        """
        creates lists when overlapping keys are found to save all values
        ensures that "id_key" does not result in a list being created
        """
        for doc in individual_json_list:
            loaded = json.loads(doc)
            for key, value in loaded.items():
                existing_value = final_json.get(key)
                if key == self.id_key:
                    if existing_value:
                        continue
                    else:
                        final_json[key] = value

                else:
                    type(self)._do_safe_update(final_json, existing_value, key, value)

    @staticmethod
    def _do_safe_update(final_json, existing_value, key, value):
        if existing_value and isinstance(existing_value, list):
            final_json[key].append(value)
        elif existing_value:
            final_json[key] = [existing_value, value]
        else:
            final_json[key] = value


dispatch(Joiner)
