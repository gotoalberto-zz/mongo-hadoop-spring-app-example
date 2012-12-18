/**
*    mongo-hadoop-spring-app-example
*    Copyright (C) 2012
*    @author Alberto Gomez Toribio
*    
*    Linkedin: http://linkedin.com/in/gotoalberto
*	 Twitter: http://twitter.com/gotoalberto
*
*    This program is free software: you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation, either version 3 of the License, or
*    (at your option) any later version.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
**/

package com.gotoalberto.mongohadoopspringappexample.util;

import java.io.IOException;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class WinLocalFileSystem extends LocalFileSystem {

	/**
	 *
	 *
	 */
	public WinLocalFileSystem() {
		super();
		System.err.println("Patch for HADOOP-7682: "+
			"Instantiating workaround file system");
	}


	/**
	 * Delegates to <code>super.mkdirs(Path)</code> and separately calls
	 * <code>this.setPermssion(Path,FsPermission)</code>
	 *
	 */
	@Override
	public boolean mkdirs(Path path, FsPermission permission)
			throws IOException {
		boolean result=super.mkdirs(path);
		this.setPermission(path,permission);
		return result;
	}


	/**
	 * Ignores IOException when attempting to set the permission
	 *
	 */
	@Override
	public void setPermission(Path path, FsPermission permission)
			throws IOException {
		try {
			super.setPermission(path,permission);
		}
		catch (IOException e) {
			System.err.println("Patch for HADOOP-7682: "+
				"Ignoring IOException setting persmission for path \""+path+
				"\": "+e.getMessage());
		}
	}
}
