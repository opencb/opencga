package org.opencb.opencga.account.io.result;

import org.bioinfo.commons.io.utils.IOUtils;
import org.bioinfo.commons.utils.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class Result {

    private List<Item> metaItems;
    private List<Item> inputItems;
    private List<Item> outputItems;

    public Result() {
        this(null);
    }

    public Result(String filename) {
        this.metaItems = new ArrayList<Item>();
        this.inputItems = new ArrayList<Item>();
        this.outputItems = new ArrayList<Item>();
    }

    public void addMetaItem(Item item) {
        metaItems.add(item);
    }

    public void addInputItem(Item item) {
        inputItems.add(item);
    }

    public void addOutputItem(Item item) {
        outputItems.add(item);
    }


    /*
     * Working with TEXT
     */
    public void saveAsText(String fileName) throws IOException {
        if (fileName == null) {
            throw new IOException("result filename undefined");
        }
        IOUtils.write(fileName, this.toString());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(listItemToString(metaItems));
        sb.append(listItemToString(inputItems));
        sb.append(listItemToString(outputItems));
        return sb.toString().trim();
    }

    private String listItemToString(List<Item> items) {
        StringBuilder sb = new StringBuilder();
        for (Item item : items) {
            sb.append(item.toString()).append("\n");
        }
        return sb.toString();
    }

    /*
     * writing in XML
     */
    public void saveAsXml(String fileName) throws IOException {
        XMLWriter writer = new XMLWriter(new FileWriter(fileName), OutputFormat.createPrettyPrint());
        writer.write(getXmlDocument());
        writer.close();
    }

    public String toXml() {
        return getXmlDocument().asXML();
    }

    private Document getXmlDocument() {
        Document doc = DocumentHelper.createDocument();
        Element root = doc.addElement("result");
        addElements(root, "metadata", metaItems);
        addElements(root, "input", inputItems);
        addElements(root, "output", outputItems);
        return doc;
    }

    private void addElements(Element parent, String name, List<Item> items) {
        if (items == null || items.size() == 0) {
            return;
        }
        StringBuilder tags = new StringBuilder("");
        StringBuilder style = new StringBuilder("");
        String[] keys;
        Element parentElement, elementItem;
        parentElement = parent.addElement(name);
        for (Item item : items) {
            if (item.getTags().size() > 0) {
                for (int i = 0; i < item.getTags().size() - 1; i++) {
                    tags.append(item.getTags().get(i)).append(",");
                }
                tags.append(item.getTags().get(item.getTags().size() - 1));
            }
            if (item.getStyle().size() > 0) {
                Iterator it = item.getStyle().entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pairs = (Map.Entry) it.next();
                    style.append(pairs.getKey()).append(":").append(pairs.getValue()).append(",");
                }
                style.deleteCharAt(style.length() - 1);
            }

            elementItem = parentElement.addElement("item");
            elementItem.addAttribute("name", item.getName());
            elementItem.addAttribute("title", item.getTitle());
            elementItem.addAttribute("type", item.getType().toString());
            if (tags == null || tags.toString() == null) {
                elementItem.addAttribute("tags", "");
            } else {
                elementItem.addAttribute("tags", tags.toString());
            }

            elementItem.addAttribute("style", style.toString());
            elementItem.addAttribute("group", item.getGroup());
            elementItem.addAttribute("context", item.getContext());
            if (item.getValue() == null) {
                elementItem.addText("");
            } else {
                elementItem.addText(item.getValue());
            }

            // clear the tags and style
            tags.delete(0, tags.length());
            style.delete(0, style.length());
        }
    }

    /*
     * reading in XML
     */
    public void loadXmlFile(String fileName) throws DocumentException, IOException {
        String xml = IOUtils.toString(fileName);
        loadXml(xml);
    }

    @SuppressWarnings("unchecked")
    public void loadXml(String xml) throws DocumentException {
        String name;
        Element root, element;
        Document doc = DocumentHelper.parseText(xml);
        root = doc.getRootElement();
        for (Iterator<Element> iter = root.elementIterator(); iter.hasNext(); ) {
            element = iter.next();
            name = element.getQualifiedName();
            if (name.equalsIgnoreCase("metadata")) {
                setItems(element, metaItems);
            } else if (name.equalsIgnoreCase("input")) {
                setItems(element, inputItems);
            } else if (name.equalsIgnoreCase("output")) {
                setItems(element, outputItems);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void setItems(Element element, List<Item> items) {
        Element e;
        Item newItem;
        Iterator<Element> i = element.elementIterator();
        while (i.hasNext()) {
            e = i.next();
            if (e.getQualifiedName().equalsIgnoreCase("item")) {
                newItem = new Item(e.attributeValue("name"), e.getText(), e.attributeValue("title"), Item.TYPE.valueOf(e.attributeValue("type")));

                if (e.attributeValue("tags") != null && !e.attributeValue("tags").equals("")) {
                    List<String> values = StringUtils.toList(e.attributeValue("tags").trim());
                    for (int j = 0; j < values.size(); j++) {
                        newItem.addTag(values.get(j));
                    }
                }

                if (e.attributeValue("style") != null && !e.attributeValue("style").equals("")) {
                    List<String> values = StringUtils.toList(e.attributeValue("style").trim());
                    for (int j = 0; j < values.size(); j += 2) {
                        newItem.addStyle(values.get(j), values.get(j + 1));
                    }
                }

                if (e.attribute("group") != null) {
                    newItem.setGroup(e.attributeValue("group"));
                }

                items.add(newItem);
            }
        }
    }


    /**
     * @param metaItems the metaItems to set
     */
    public void setMetaItems(List<Item> metaItems) {
        this.metaItems = metaItems;
    }

    /**
     * @return the metaItems
     */
    public List<Item> getMetaItems() {
        return metaItems;
    }

    /**
     * @param inputItems the inputItems to set
     */
    public void setInputItems(List<Item> inputItems) {
        this.inputItems = inputItems;
    }

    /**
     * @return the inputItems
     */
    public List<Item> getInputItems() {
        return inputItems;
    }

    /**
     * @param outputItems the outputItems to set
     */
    public void setOutputItems(List<Item> outputItems) {
        this.outputItems = outputItems;
    }

    /**
     * @return the outputItems
     */
    public List<Item> getOutputItems() {
        return outputItems;
    }

}

