/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.app.localserver;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public class GetFoldersServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setHeader("Access-Control-Allow-Origin", "*");
        PrintWriter pw = resp.getWriter();

        StringBuilder respStr = new StringBuilder("[");
        String[] folders = LocalServer.properties.getProperty("OPENCGA.LOCAL.FOLDERS.ALLOWED").split(",");
        for (String folder : folders) {
            Path path = Paths.get(folder);
//			respStr.append(listRecursiveJson(path).toString()).append(',');
            respStr.append(getFileTree(path)).append(',');
        }
        respStr.deleteCharAt(respStr.length() - 1);
        respStr.deleteCharAt(respStr.length() - 1);
        respStr.append("]");

        pw.write(respStr.toString().replace(",,", ","));
        pw.close();
    }

//	private StringBuilder listRecursiveJson(Path path) throws IOException {
//		return listRecursiveJson(path, false);
//	}
//
//	private StringBuilder listRecursiveJson(Path filePath, boolean coma) throws IOException {
//		String c = "\"";
//		StringBuilder sb = new StringBuilder();
//		if (coma) {
//			sb.append(",");
//		}
//		sb.append("{");
//		sb.append(c + "text" + c + ":" + c + filePath.getFileName()+ c + ",");
//		sb.append(c + "oid" + c + ":" + c + filePath.toAbsolutePath()+ c);
//		if (Files.isDirectory(filePath)) {
//			sb.append(",");
//			sb.append(c + "children" + c + ":[");
//
//			DirectoryStream.Filter<Path> filter = new DirectoryStream.Filter<Path>() {
//				public boolean accept(Path file) throws IOException {
//					System.out.println(">>"+file.toAbsolutePath());
//					if(Files.isReadable(file) && (Files.isDirectory(file) || file.endsWith(".bam"))) {
//						System.out.println("*********************+"+file.toAbsolutePath());
//						return true;						
//					}else {
//						return false;						
//					}
//				}
//			};
//
//			DirectoryStream<Path> folderStream = Files.newDirectoryStream(filePath, filter);
//			int i = 0;
//			for (Path p : folderStream) {
//				System.out.println(p.toAbsolutePath());
//				if (i == 0) {
//					sb.append(listRecursiveJson(p, false));
//				} else {
//					sb.append(listRecursiveJson(p, true));
//				}
//				i++;
//			}
//			return sb.append("]}");
//		}
//		sb.append(",");
//		sb.append(c + "iconCls" + c + ":" + c + "icon-regular-file" + c + ",");
//		sb.append(c + "leaf" + c + ":" + c + "true" + c);
//		return sb.append("}");
//	}


    public String getFileTree(final Path filePath) throws IOException {

        final String c = "\"";
        final StringBuilder sb = new StringBuilder();

        Files.walkFileTree(filePath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
//				System.out.println(">>>>>>>>>>>>>>>>>>>>>>"+file.toString());
                if (!Files.isDirectory(file) && (file.toString().endsWith(".vcf") || file.toString().endsWith(".bam"))) {
                    sb.append("{");
                    sb.append(c + "text" + c + ":" + c + file.getFileName() + c + ",");
                    sb.append(c + "oid" + c + ":" + c + file.toAbsolutePath() + c);
                    sb.append(",");
                    sb.append(c + "iconCls" + c + ":" + c + "icon-regular-file" + c + ",");
                    sb.append(c + "leaf" + c + ":" + c + "true" + c);
                    sb.append("},");
//					System.out.println("*********************+"+file.toAbsolutePath());
                    return FileVisitResult.CONTINUE;
                } else {
                    return FileVisitResult.CONTINUE;
                }
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                // try to delete the file anyway, even if its attributes
                // could not be read, since delete-only access is
                // theoretically possible

                return FileVisitResult.SKIP_SUBTREE;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                if (Files.isHidden(dir) || !Files.isReadable(dir)) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                sb.append("{");
                sb.append(c + "text" + c + ":" + c + dir.getFileName() + c + ",");
                sb.append(c + "oid" + c + ":" + c + dir.toAbsolutePath() + c + ",");
                sb.append(c + "children" + c + ":[");
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (Files.isHidden(dir) || !Files.isReadable(dir)) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                sb.append("]},");
                return FileVisitResult.CONTINUE;
            }
        });
        return sb.toString().replace("},]", "}]");
    }
}
