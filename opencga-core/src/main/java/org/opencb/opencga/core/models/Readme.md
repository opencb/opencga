# Generate Markdown models with Zetta2Markdown

## Overview

The zetta2Markdown is a doclet to document the datamodels automatically in a markdown syntax using the javadoc comments.

## General build process

For use this doclet you need to add an invocation like this of the maven-javadoc-plugin in the pom.xml

```javascript
<plugin>  
	 <groupId>org.apache.maven.plugins</groupId>  
	 <artifactId>maven-javadoc-plugin</artifactId>  
	 <version>3.3.0</version>  
	 <executions> 
		 <execution> 
			 <id>attach-javadocs</id>
			 <phase>package</phase>  
			 <goals> 
				 <goal>jar</goal>  
			 </goals> 
		</execution> 
	</executions> 
	<configuration> 
		<doclet>com.zetta.javadoc.doclet.MarkdownModelDoclet</doclet>  
		<docletArtifact> 
			<groupId>com.zetta</groupId>  
			 <artifactId>ZettaJavaDoc</artifactId>  
			 <version>1.0</version>  
		 </docletArtifact> 
		 <additionalOptions> 
			 <additionalOption>-outputdir</additionalOption>  
			 <additionalOption>${project.basedir}/src/main/resources/doc</additionalOption>  
			 <additionalOption>-classes2Markdown</additionalOption>  
			 <additionalOption>
			 org.opencb.opencga.core.models.sample.Sample;org.opencb.opencga.core.models.sample.SampleCollection
			 </additionalOption>  
			 <additionalOption>-tableTagsClasses</additionalOption>  
			 <additionalOption>
			 org.opencb.opencga.core.models.sample.Sample;org.opencb.opencga.core.models.sample.SampleCollection
			 </additionalOption>  
			 <additionalOption>-jsondir</additionalOption>  
			 <additionalOption>${project.basedir}/src/main/resources/doc/json</additionalOption>  
		 </additionalOptions> 
		 <groups> 
			 <group>
				 <title>Sample DataModel Packages</title>
				 <packages>org.opencb.opencga.core.models</packages>  
			 </group> 
		</groups> 
	</configuration>
</plugin>
```

## Options

To use this doclet you can use the common options of the javaDoc and must use mandatory options for Zetta2Markdown. The optios of the doclet
are:

- **outputdir** The directory where the doclet must write the "*.md*" files
- **classes2Markdown** The classes you want to document
- **jsondir** Directory for stored json examples. If you want the document has a json example section, it is not required parameter
- **githubServer** The server's url of github where are the source code
- **tableTagsClasses** classes where must apair the fields properties table

## Class comments

The comments in class should look like this:

```javascript
/**  
 * descritpion .... 
 * @implNote You can update before annotation 2.2  
 * @deprecated  
 * @see [ZetaGenomics] (https://www.zettagenomics.com)  
 */
```

The first paragraph is the class description, it is used to fill the overview section of the document.
**ImplNote** is used to comment the code. The tag **deprecated** serves to warn the user that it is not recommended to use this class.
The **see** tag is used to link some url to complete the information

## Fields comments

Comments of fields should look like this:

```javascript
/**  
 * descritpion .... 
 * @apiNote Required Immutable Unique Internal
 * @implNote You can update before annotation 2.2  
 * @since 2.1  
 * @deprecated  
 * @see [ZetaGenomics] (https://www.zettagenomics.com)  
 */
```

The first paragraph is the field description it must explain what the field means

The tags of the **apiNote** means that:

* If the field is not internal and is not immutable either it will appear in the updatable fields list
* If the field has the immutable tag but does not have the internal tag, it will appear in the creation list
* If the field has the required tag indicates a mandatory field that must be sent in the creation phase
* If the field has the internal tag indicates that it is managed directly by the application and that the user cannot modify

**implNote** Contains some comment that should not be in the description of the field but is relevant in terms of code, release note, its
relationship with other classes, etc.

**since** contains the class release number when the field was added to the class. The tag **deprecated** serves to warn the user that it is
not recommended to use this field. The **see** tag is used to link some url to complete the information.