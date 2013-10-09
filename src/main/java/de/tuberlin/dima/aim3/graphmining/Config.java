/**
 * Graph-Mining Tutorial for Ozone
 *
 * Copyright (C) 2013  Sebastian Schelter <ssc@apache.org>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.aim3.graphmining;

public class Config {

  private Config() {}

  public static int numberOfSubtasks() {
    return 1;
  }

  public static String pathToSlashdotZoo() {
    return "file:///home/ssc/Desktop/tmp/slashdot-zoo/slashdot-zoo.csv";
  }

  public static String outputPath() {
    return "file:///tmp/ozone/";
  }

  public static long randomSeed() {
    return 0xdeadbeef;
  }
}
