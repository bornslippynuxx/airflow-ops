/**
 * Console output. Status/progress goes to stderr so that a command's real
 * result (on stdout) stays clean and pipeable.
 */
export class Log {
  static status(message: string): void {
    process.stderr.write(message + "\n");
  }

  static result(message: string): void {
    process.stdout.write(message + "\n");
  }
}
