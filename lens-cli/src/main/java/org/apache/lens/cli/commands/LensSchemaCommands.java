package org.apache.lens.cli.commands;

import java.io.*;
import java.util.List;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.CommandResult;
import org.springframework.shell.core.JLineShellComponent;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.shell.support.logging.HandlerUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.google.common.collect.Lists;

@Component
public class LensSchemaCommands implements CommandMarker {
  protected final Logger logger = HandlerUtils.getLogger(getClass());

  @Autowired
  private ApplicationContext applicationContext;
  FilenameFilter filter = new FilenameFilter() {
    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(".xml");
    }
  };
  @Autowired
  private JLineShellComponent shell;

  @CliCommand(value = {"schema"}, help = "Parses the specified resource file and executes its commands")
  public void script(
    @CliOption(key = {"", "db"},
      help = "The database to create schema in", mandatory = true) final String database,
    @CliOption(key = {"", "file"},
      help = "The parent directory of the schema", mandatory = true) final File schemaDirectory) {
    if (!schemaDirectory.isDirectory()) {
      throw new IllegalStateException("Schema directory should be a directory");
    }
    CommandResult result;

    // ignore result. it can fail if database already exists
    shell.executeCommand("create database " + database);
    if (shell.executeScriptLine("use " + database)) {
      createOrUpdate(new File(schemaDirectory, "storages"), "storage",
        "create storage --path %s", "update storage --name %s --path %s");
      createOrUpdate(new File(schemaDirectory, "dimensions"), "dimension",
        "create dimension --path %s", "update dimension --name %s --path %s");
      createOrUpdate(new File(new File(schemaDirectory, "cubes"), "base"), "base cube",
        "create cube --path %s", "update cube --name %s --path %s");
      createOrUpdate(new File(new File(schemaDirectory, "cubes"), "derived"), "derived cube",
        "create cube --path %s", "update cube --name %s --path %s");
      createOrUpdate(new File(schemaDirectory, "dimensiontables"), "dimension table",
        "create dimtable --path %s", "update dimtable --dimtable_name %s --path %s");
      createOrUpdate(new File(schemaDirectory, "facts"), "fact",
        "create fact --path %s", "update dimtable --fact_name %s --path %s");
    }
  }

  public List<File> createOrUpdate(File parent, String entityType, String createSyntax, String updateSyntax) {
    List<File> failedFiles = Lists.newArrayList();
    // Create/update entities
    if (parent.exists()) {
      Assert.isTrue(parent.isDirectory(), parent.toString() + " must be a directory");
      for (File entityFile : parent.listFiles(filter)) {
        String entityName = entityFile.getName().substring(0, entityFile.getName().length() - 3);
        String entityPath = entityFile.getAbsolutePath();
        if (shell.executeScriptLine(String.format(createSyntax, entityPath))) {
          logger.info("Created " + entityType + " " + entityName);
        } else if (shell.executeScriptLine(String.format(updateSyntax, entityName, entityPath))) {
          logger.info("Updated " + entityType + " " + entityName);
        } else {
          logger.severe("Couldn't create or update " + entityType + " " + entityName);
          failedFiles.add(entityFile);
        }
      }
    }
    return failedFiles;
  }
}