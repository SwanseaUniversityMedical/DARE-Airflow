from datetime import datetime
import re
from jinja2 import Template


class AttributeSearch:
    def __init__(self, source, regex, single):
        self.source = source
        self.regex = regex
        self.single = single

    def get_structure(self):
        return {
            "source": self.source,
            "regex": self.regex,
            "single": self.single
        }


def compute_params(s3, attribs, templates):
    # add system attributes
    formatted_date = datetime.today().strftime("%Y%m%d")
    print(f"Compute Params Attribs = {attribs}")

    # compute value of add attributes and add

    attrib_values = dict()
    for key, value in attribs.items():
        if isinstance(value, AttributeSearch):
            structure = value.get_structure()
            template = Template(structure["source"])
            searchstring = template.render(s3=s3, attrib=attrib_values)
            result = ""
            if structure["regex"]:
                if structure["single"] is True:
                    match = re.search(structure["regex"], searchstring)
                    if match:
                        result = match.group()
                else:
                    result = re.findall(structure["regex"], searchstring)
            else:
                result = searchstring  # allow literals

            attrib_values[key] = result
            print(f"Attribute search {key} : {structure} ==> {result}")

    print(f"Compute Ledger Attribute Values : {attrib_values}")

    t0 = Template(templates['dataset_template'])
    dataset_name = t0.render(s3=s3, attrib=attrib_values, date=formatted_date)
    attrib_values["dataset"] = dataset_name

    t1 = Template(templates['version_template'])
    version = t1.render(s3=s3, attrib=attrib_values, date=formatted_date)
    attrib_values["version"] = version

    t2 = Template(templates['label_template'])
    label = t2.render(s3=s3, attrib=attrib_values, date=formatted_date)
    attrib_values["label"] = label

    t3 = Template(templates['table_template'])
    table_name = t3.render(s3=s3, attrib=attrib_values, date=formatted_date)

    return dict(
        dataset=dataset_name,
        version=version,
        tablename=table_name,
        label=label
    )
