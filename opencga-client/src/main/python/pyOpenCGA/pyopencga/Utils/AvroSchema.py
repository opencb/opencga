import json

__author__ = 'antonior'


class AvroSchemaFile:
    def __init__(self, filename):
        self.filename = filename
        self.data = json.load(open(self.filename))

    def get_external_reference(self, field_name):
        external_file = self.filename.split('/')[:-1]
        external_file.append(field_name + '.avsc')
        external_file = open('/'.join(external_file))
        return json.load(external_file)

    def convert_variable_set(self, data):
        variable_set = []
        if "fields" in data:
            fields = data["fields"]

            for field in fields:
                self.convert_field(field, variable_set)
        return variable_set

    def convert_field(self, field, variable_set):
        required = True
        multi = False
        field_type = field["type"]
        field_name = field["name"]
        print (" Parse field : %s " % field_name)
        try:
            desc = field["doc"]
        except:
            desc = ""
        try:
            default = field["default"]
        except KeyError:
            default = None
        if isinstance(field_type, list):
            if "null" in field_type:
                field_type.remove("null")
                required = False

            if len(field_type) > 1:
                print(
                    "This model does not accept multiple type, only the first one will be stored: in the result model " + field_name + " always has to be " +
                    field_type[0])

            field_type = field_type[0]
            field["type"] = field_type

        if isinstance(field_type, dict):
            self.process_complex_field(default, desc, field, field_name, field_type, multi, required, variable_set)

        elif field_type not in ['record', 'string', 'int', 'float', 'double', 'map', 'enum', 'boolean']:
            external_type = self.get_external_reference(field_type)
            field["type"] = external_type
            field_type = external_type
            self.process_complex_field(default, desc, field, field_name, field_type, multi, required, variable_set)

        else:
            self.parse_basic_types(desc, field, field_name, field_type, required, variable_set, multi, default)

    def process_complex_field(self, default, desc, field, field_name, field_type, multi, required, variable_set):
        if field_type["type"] == "array":
            multi = True
            if isinstance(field_type["items"], dict):
                field = field_type["items"]
                field_type = field["type"]
                self.parse_basic_types(desc, field, field_name, field_type, required, variable_set, multi, default)
            else:
                field_type = field_type["items"]
                self.parse_basic_types(desc, field, field_name, field_type, required, variable_set, multi, default)

        elif field_type["type"] == "record":
            field = field_type
            field_type = field_type["type"]
            self.parse_basic_types(desc, field, field_name, field_type, required, variable_set, multi, default)

        elif field_type["type"] == "enum":
            field_type = field_type["type"]
            self.parse_basic_types(desc, field, field_name, field_type, required, variable_set, multi, default)

        elif field_type["type"] == "map":
            field_type = field_type["type"]
            self.parse_basic_types(desc, field, field_name, field_type, required, variable_set, multi, default)

    # This function is recursive take care!!!
    def parse_basic_types(self, desc, field, field_name, field_type, required, variable_set, multi, default=None):

        if field_type == "record":
            fields = field["fields"]
            variable_set.append({"name": field_name, "required": required, "type": 'OBJECT', "description": desc,
                                 "variableSet": self.convert_variable_set(field), "multiValue": multi})
        elif field_type == "string":
            variable_set.append(
                {"name": field_name, "required": required, "type": 'TEXT', "description": desc, "multiValue": multi})
            if default is not None:
                variable_set[-1]["defaultValue"] = default

        elif field_type == "float" or field_type == "double":
            variable_set.append(
                {"name": field_name, "required": required, "type": 'DOUBLE', "description": desc, "multiValue": multi})
            if default is not None:
                variable_set[-1]["defaultValue"] = default

        elif field_type == "int":
            variable_set.append(
                {"name": field_name, "required": required, "type": 'INTEGER', "description": desc, "multiValue": multi})
            if default is not None:
                variable_set[-1]["defaultValue"] = default

        elif field_type == "boolean":
            variable_set.append(
                {"name": field_name, "required": required, "type": 'BOOLEAN', "description": desc, "multiValue": multi})
            if default is not None:
                variable_set[-1]["defaultValue"] = default

        elif field_type == "map":
            variable_set.append(
                {"name": field_name, "required": required, "type": 'OBJECT', "description": desc, "multiValue": multi})
            if default is not None:
                variable_set[-1]["defaultValue"] = default

        elif field_type == "enum":
            try:
                symbols = field["type"]["symbols"]
                variable_set.append(
                    {"name": field_name, "required": required, "type": 'CATEGORICAL', "description": desc,
                     "allowedValues": symbols, "multiValue": multi})
                if default is not None:
                    variable_set[-1]["defaultValue"] = default
            except:
                symbols = field["symbols"]
                variable_set.append(
                    {"name": field_name, "required": required, "type": 'CATEGORICAL', "description": desc,
                     "allowedValues": symbols, "multiValue": multi})
                if default is not None:
                    variable_set[-1]["defaultValue"] = default
        else:
            raise Exception('Uncaught Field Type: ' + str(field_type))


"""
This is how this work

a = AvroSchemaFile("/home/antonior/PycharmGEL/GelReportModels/schemas/JSONs/RDParticipant/RDParticipant.avsc")
print (a.convert_variable_set(a.data))

"""
