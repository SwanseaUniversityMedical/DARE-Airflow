import re
from jinja2 import Template

class attribute_search:
  def __init__(self, source, regex):
    self.source = source
    self.regex = regex

  def get_structure(self):
        return {
            "source": self.source,
            "regex": self.regex
        }
  
def compute_ledger(s3, attribs):
    
    #add system attributes

    # add attribute that would come from form.io
    attribs["att_version"]= attribute_search("{{s3.filename}}", r'''(?<=ODV)\d''')
    attribs["att_label"]= attribute_search("{{s3.filename}}", r'''\w+(?=_ODV)''')
    print(f"Compute Ledger Attribs = {attribs}")

    # compute value of add attributes and add
    
    attrib_values = dict()
    for key, value in attribs.items():
        if isinstance(value, attribute_search):
            structure = value.get_structure()
            template = Template(structure["source"])
            searchstring = template.render(s3=s3, attrib=attrib_values)
            if structure["regex"]:
                attrib_values[key] = re.findall(structure["regex"],searchstring)
            else:
                attrib_values[key] = searchstring  # allow literals

    print(f"Compute Ledger Attribute Values : {attrib_values}")


    version_template=r'''{% if (s3.version) and s3.version %}{{ s3.version}}{% else %}{{ attrib["att_version"][0] }}{% endif %}'''
    #label_template = '{{ attrib["att_label"][0] }}'
    label_template = '{{ s3.filename }}'
    table_template = '{{ attrib["label"] }}'

    t1 = Template(version_template)
    version = t1.render(s3=s3, attrib=attrib_values)
    attrib_values["version"]=version

    t2 = Template(label_template)
    label = t2.render(s3=s3, attrib=attrib_values)
    attrib_values["label"]=label
    
    t3 = Template(table_template)
    tablename = t3.render(s3=s3, attrib=attrib_values)

    return dict (
        version=version,
        tablename = tablename,
        label = label
    )